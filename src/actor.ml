(*****************************************************************************)
(*                                                                           *)
(* Open Source License                                                       *)
(* Copyright (c) 2018 Dynamic Ledger Solutions, Inc. <contact@tezos.com>     *)
(*                                                                           *)
(* Permission is hereby granted, free of charge, to any person obtaining a   *)
(* copy of this software and associated documentation files (the "Software"),*)
(* to deal in the Software without restriction, including without limitation *)
(* the rights to use, copy, modify, merge, publish, distribute, sublicense,  *)
(* and/or sell copies of the Software, and to permit persons to whom the     *)
(* Software is furnished to do so, subject to the following conditions:      *)
(*                                                                           *)
(* The above copyright notice and this permission notice shall be included   *)
(* in all copies or substantial portions of the Software.                    *)
(*                                                                           *)
(* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR*)
(* IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,  *)
(* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL   *)
(* THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER*)
(* LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING   *)
(* FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER       *)
(* DEALINGS IN THE SOFTWARE.                                                 *)
(*                                                                           *)
(*****************************************************************************)

open Core
open Async

module S = Actor_s
module Types = Actor_types

open S

module Make
    (Name : NAME)
    (Event : EVENT)
    (Request : REQUEST)
    (Types : TYPES) = struct

  module Name = Name
  module Event = Event
  module Request = Request
  module Types = Types

  module Timestamped_evt = struct
    type t = {
      ts : Time_ns.t ;
      evt: Event.t ;
    }

    let dummy = {
      ts = Time_ns.epoch ;
      evt = Event.dummy ;
    }

    let create calibrator evt = {
      ts = Time_stamp_counter.to_time_ns ~calibrator
          (Time_stamp_counter.now ()) ; evt }
  end

  module EventRing = CCRingBuffer.Make(Timestamped_evt)

  let base_name = String.concat ~sep:"." Name.base

  type message = Message: 'a Request.t * 'a Or_error.t Ivar.t option -> message

  type bounded
  type infinite
  type 'a queue

  type dropbox

  type _ buffer_kind =
    | Queue : infinite queue buffer_kind
    | Bounded : { size : int } -> bounded queue buffer_kind
    | Dropbox :
        { merge : (dropbox t ->
                   any_request ->
                   any_request option ->
                   any_request option) }
        -> dropbox buffer_kind
  and any_request = Any_request : _ Request.t -> any_request

  and _ buffer =
    | Queue_buffer :
        (Time_ns.t * message) Pipe.Reader.t *
        (Time_ns.t * message) Pipe.Writer.t -> infinite queue buffer
    | Bounded_buffer :
        (Time_ns.t * message) Pipe.Reader.t *
        (Time_ns.t * message) Pipe.Writer.t -> bounded queue buffer
    | Dropbox_buffer :
        (Time_ns.t * message) Mvar.Read_write.t -> dropbox buffer

  and 'kind t = {
    limits : Actor_types.limits ;
    timeout : Time_ns.Span.t option ;
    parameters : Types.parameters ;
    mutable (* only for init *) state : Types.state option ;
    mutable (* only for init *) monitor : Monitor.t ;
    buffer : 'kind buffer ;
    event_log : (Logs.level * EventRing.t) list ;
    logger : (module Logs_async.LOG) ;
    name : Name.t ;
    id : int ;
    mutable status : Actor_types.worker_status ;
    mutable current_request : (Time_ns.t * Time_ns.t * Request.view) option ;
    table : 'kind table ;
    terminating : unit Ivar.t ;
    cleaned : unit Ivar.t ;

    calibrator : Time_stamp_counter.Calibrator.t ;
    warp10 : Warp10.t Pipe.Writer.t ;
    http_server : (Socket.Address.Inet.t, int) Tcp.Server.t option ;
  }
  and 'kind table = {
    buffer_kind : 'kind buffer_kind ;
    mutable last_id : int ;
    instances : (Name.t, 'kind t) Hashtbl.t ;
    zombies : (int, 'kind t) Hashtbl.t
  }

  exception Closed of Name.t
  exception Exit_worker_loop of Error.t option

  let may_raise_closed w =
    if Ivar.is_full w.terminating then raise (Closed w.name)

  let queue_item ?iv r =
    Time_ns.now (),
    Message (r, iv)

  let drop_request (w : dropbox t) request =
    may_raise_closed w ;
    let Dropbox { merge } = w.table.buffer_kind in
    let Dropbox_buffer message_box = w.buffer in
    don't_wait_for @@ Monitor.handle_errors begin fun () ->
      match
        match Mvar.peek message_box with
        | None ->
          merge w (Any_request request) None
        | Some (_, Message (old, _)) ->
          Deferred.(don't_wait_for (ignore (Mvar.take message_box))) ;
          merge w (Any_request request) (Some (Any_request old))
      with
      | None -> Deferred.unit
      | Some (Any_request neu) ->
        Mvar.put message_box (Time_ns.now (), Message (neu, None))
    end (fun _ -> ())

  let push_request (type a) (w : a queue t) request =
    may_raise_closed w ;
    match w.buffer with
    | Queue_buffer (_, message_queue) ->
      Pipe.write message_queue (queue_item request)
    | Bounded_buffer (_, message_queue) ->
      Pipe.write message_queue (queue_item request)

  let push_request_now (w : infinite queue t) request =
    may_raise_closed w ;
    let Queue_buffer (_, message_queue) = w.buffer in
    Pipe.write_without_pushback message_queue (queue_item request)

  let try_push_request_now (w : bounded queue t) request =
    may_raise_closed w ;
    let Bounded_buffer (_, message_queue) = w.buffer in
    let Bounded { size } = w.table.buffer_kind in
    let qsize = Pipe.length message_queue in
    if qsize < size then begin
      Pipe.write_without_pushback message_queue (queue_item request) ;
      true
    end
    else false

  let push_request_and_wait (type a) (w : a queue t) request =
    may_raise_closed w ;
    let message_queue = match w.buffer with
      | Queue_buffer (_, message_queue) -> message_queue
      | Bounded_buffer (_, message_queue) -> message_queue in
    let iv = Ivar.create () in
    Pipe.write message_queue (queue_item ~iv request) >>= fun () ->
    Ivar.read iv

  let pop (type a) (w : a t) =
    let pop_queue message_queue =
      match w.timeout with
      | None -> begin
          Pipe.read message_queue >>= function
          | `Eof -> return None
          | `Ok m -> return (Some m)
        end
      | Some timeout ->
        Clock_ns.with_timeout
          timeout (Pipe.read message_queue) >>= function
        | `Timeout
        | `Result `Eof -> return None
        | `Result (`Ok m) -> return (Some m) in
    may_raise_closed w ;
    match w.buffer with
    | Queue_buffer (message_queue, _) -> pop_queue message_queue
    | Bounded_buffer (message_queue, _) -> pop_queue message_queue
    | Dropbox_buffer message_box ->
      match w.timeout with
      | None ->
        Mvar.take message_box >>= fun m ->
        return (Some m)
      | Some timeout ->
        Clock_ns.with_timeout
          timeout (Mvar.take message_box) >>= function
        | `Timeout ->  return None
        | `Result m -> return (Some m)

  let trigger_shutdown w =
    may_raise_closed w ;
    Monitor.send_exn w.monitor (Exit_worker_loop None)

  let monitor w =
    may_raise_closed w ;
    w.monitor

  let record_event w evt =
    may_raise_closed w ;
    let level = Event.level evt in
    if level >= w.limits.backlog_level then
      EventRing.push_back
        (List.Assoc.find_exn ~equal:(=) w.event_log level)
        (Timestamped_evt.create w.calibrator evt)

  let log_event w evt =
    record_event w evt ;
    let level = Event.level evt in
    let (module Logger) = w.logger in
    begin match Event.to_warp10 evt with
      | Some m when level >= w.limits.backlog_level ->
        Pipe.write_if_open w.warp10 m
      | _ -> Deferred.unit
    end >>= fun () ->
    Logger.msg level (fun m -> m "@[<v 0>%a@]" Event.pp evt)

  let log_event_now w evt =
    don't_wait_for (log_event w evt)

  module type HANDLERS = sig
    type self
    val on_launch :
      self -> Name.t -> Types.parameters -> Types.state Deferred.t
    val on_request :
      self -> 'a Request.t -> 'a Deferred.t
    val on_no_request :
      self -> unit Deferred.t
    val on_close :
      self -> unit Deferred.t
    val on_error :
      self -> Request.view -> Actor_types.request_status ->
      Error.t -> unit Deferred.t
    val on_completion :
      self -> 'a Request.t -> 'a -> Actor_types.request_status ->
      unit Deferred.t
  end

  let create_table buffer_kind =
    { buffer_kind ;
      last_id = 0 ;
      instances = Hashtbl.Poly.create () ;
      zombies = Hashtbl.Poly.create () }

  let queue = create_table Queue
  let bounded size = create_table (Bounded { size })
  let dropbox merge = create_table (Dropbox { merge })

  let cleanup_worker (type kind) handlers (w : kind t) err =
    let (module Logger) = w.logger in
    let close (type a) (w : a t) =
      let wakeup = function
        | _, Message (_, Some iv) ->
          Ivar.fill iv (Error (Error.of_exn (Closed w.name)))
        | _ -> () in
      let close_queue message_queue =
        match Pipe.read_now' message_queue with
        | `Eof | `Nothing_available -> ()
        | `Ok messages ->
          Queue.iter ~f:wakeup messages ;
          Pipe.close_read message_queue in
      match w.buffer with
      | Queue_buffer (message_queue, _) -> close_queue message_queue
      | Bounded_buffer (message_queue, _) -> close_queue message_queue
      | Dropbox_buffer message_box ->
        Option.iter ~f:wakeup (Mvar.peek message_box) in
    let (module Handlers : HANDLERS with type self = kind t) = handlers in
    let t0 = match w.status with
      | Running t0 -> t0
      | _ -> assert false in
    w.status <- Closing (t0, Time_ns.now ()) ;
    Logger.debug (fun m -> m "Now in Closing status") >>= fun () ->
    close w ;
    w.status <- Closed (t0, Time_ns.now (), err) ;
    Logger.debug (fun m -> m "Now in Closed status") >>= fun () ->
    Hashtbl.remove w.table.instances w.name ;
    Handlers.on_close w >>= fun () ->
    w.state <- None ;
    Hashtbl.Poly.set w.table.zombies ~key:w.id ~data:w ;
    don't_wait_for begin
      Clock_ns.after w.limits.zombie_memory >>= fun () ->
      Logger.debug (fun m -> m "Cleaning up zombie memory") >>= fun () ->
      List.iter ~f:(fun (_, ring) -> EventRing.clear ring) w.event_log ;
      Clock_ns.after Time_ns.Span.(w.limits.zombie_lifetime - w.limits.zombie_memory) >>= fun () ->
      Logger.debug (fun m -> m "Removing zombie from table") >>= fun () ->
      Hashtbl.remove w.table.zombies w.id ;
      Ivar.fill w.cleaned () ;
      Logger.debug (fun m -> m "Zombies cleaned")
    end ;
    Logger.debug (fun m -> m "Worker cleaned")

  let worker_loop (type kind) handlers (w : kind t) =
    let (module Handlers : HANDLERS with type self = kind t) = handlers in
    let (module Logger) = w.logger in
    let name = Name.to_string w.name in
    let inner () =
      w.monitor <- Monitor.current () ;
      pop w >>= function
      | None -> begin
          match w.status with
          | Closing _
          | Closed _ -> (* Happens when shutdown exception is raised
                           in the current monitor. *)
            Deferred.unit
          | _ ->
            Handlers.on_no_request w
        end
      | Some (pushed, Message (request, u)) ->
        let current_request = Request.view request in
        let treated = Time_ns.now () in
        w.current_request <- Some (pushed, treated, current_request) ;
        Logger.debug begin fun m ->
          m "@[<v 2>Request:@,%a@]" Request.pp current_request
        end >>= fun () ->
        Monitor.try_with_or_error
          (fun () -> Handlers.on_request w request) >>= fun res ->
        Option.iter u ~f:(fun u -> Ivar.fill u res) ;
        let res = Or_error.ok_exn res in
        let completed = Time_ns.now () in
        w.current_request <- None ;
        Handlers.on_completion w
          request res Actor_types.{ pushed ; treated ; completed } in
    let rec loop () =
      Monitor.try_with_or_error
        ~extract_exn:true ~name inner >>= function
      | Ok () -> loop ()
      | Error err ->
        begin match Error.to_exn err with
          | Exit_worker_loop (Some err) -> Error.raise err
          | Exit_worker_loop None -> Error.raise err
          | _ -> ()
        end ;
        Logger.err (fun m -> m "%a" Error.pp err) >>= fun () ->
        Monitor.try_with_or_error ~name begin fun () ->
          match w.current_request with
          | None -> assert false
          | Some (pushed, treated, request) ->
            let completed = Time_ns.now () in
            w.current_request <- None ;
            Handlers.on_error w
              request Actor_types.{ pushed ; treated ; completed } err
        end >>= function
        | Ok () -> begin match w.status with
            | Closing _ | Closed _ -> Deferred.unit
            | _ -> loop ()
          end
        | Error e ->
          Logger.err begin fun m ->
            m "@[<v 0>Worker crashed:@,%a@]" Error.pp e
          end >>= fun () ->
          raise (Exit_worker_loop (Some e)) in
    loop ()

  let launch
    : type kind.
      kind table ->
      ?timeout:Time_ns.Span.t ->
      ?http_port:int ->
      Actor_types.limits -> Name.t -> Types.parameters ->
      (module HANDLERS with type self = kind t) ->
      kind t Deferred.t
    = fun table ?timeout ?http_port limits name parameters (module Handlers) ->
      let name_s =
        Format.asprintf "%a" Name.pp name in
      let full_name =
        if name_s = "" then base_name else Format.asprintf "%s(%s)" base_name name_s in
      let id =
        table.last_id <- table.last_id + 1 ;
        table.last_id in
      let id_name =
        if name_s = "" then base_name else Format.asprintf "%s(%d)" base_name id in
      if Hashtbl.mem table.instances name then
        invalid_arg (Format.asprintf "Worker.launch: duplicate worker %s" full_name) ;
      let buffer : kind buffer =
        match table.buffer_kind with
        | Queue ->
          let r, w = Pipe.create () in
          Queue_buffer (r, w)
        | Bounded _ ->
          let r, w = Pipe.create () in
          Bounded_buffer (r, w)
        | Dropbox _ ->
          Dropbox_buffer (Mvar.create ()) in
      let event_log =
        List.map ~f:begin fun l ->
          l, EventRing.create limits.backlog_size
        end [ Logs.App ; Error ; Warning ; Info ; Debug ] in
      let module Logger =
        (val (Logs_async.src_log (Logs.Src.create id_name))) in
      let warp10 = match Event.warp10_url with
        | None -> Pipe.create_writer (fun r -> Pipe.close_read r; Deferred.unit)
        | Some url -> begin
            let r, w = Pipe.create () in
            don't_wait_for (Warp10_async.record url r) ;
            w
          end in
      let default_error_handler _saddr ?request:_ error handle =
        let open Httpaf in
        let message =
          match error with
          | `Exn exn -> Exn.to_string exn
          | (#Status.client_error | #Status.server_error) as error ->
            Status.to_string error
        in
        let body = handle Headers.empty in
        Body.write_string body message;
        Body.close_writer body in
      let http_handler =
        Httpaf_async.Server.create_connection_handler
          ?config:None
          ~request_handler:begin fun _saddr reqd ->
            let open Httpaf in
            let headers = Headers.of_list ["Content-Type", "application/json"] in
            let resp = Response.create ~headers `OK in
            let body = Reqd.respond_with_streaming
                ~flush_headers_immediately:true
                reqd resp in
            List.iter event_log ~f:begin fun (lvl, evt) ->
              Option.iter (EventRing.peek_front evt) ~f:begin fun e ->
                Body.write_string body
                  (Format.asprintf "{\"level\": \"%a\"; \"evt\": \"%a\"}@."
                     Logs.pp_level lvl Event.pp e.evt)
              end
            end ;
            Body.close_writer body
          end
          ~error_handler:default_error_handler in
      begin match http_port with
        | None -> return None
        | Some port ->
          let open Tcp in
          Server.create_sock
            ~on_handler_error:`Ignore
            (Where_to_listen.of_port port)
            http_handler >>| Option.some
      end >>= fun http_server ->
      let w = { limits ; parameters ; name ;
                table ; buffer ; logger = (module Logger) ;
                state = None ; id ;
                monitor = Monitor.create () ; (* placeholder *)
                event_log ; timeout ;
                current_request = None ;
                status = Launching (Time_ns.now ()) ;
                terminating = Ivar.create () ;
                cleaned  = Ivar.create () ;
                calibrator = Time_stamp_counter.Calibrator.create () ;
                warp10 ;
                http_server ;
              } in
      begin
        if id_name = base_name then
          Logger.info (fun m -> m "Worker started")
        else
          Logger.info (fun m -> m "Worker started for %s" name_s)
      end >>= fun () ->
      Hashtbl.Poly.set table.instances ~key:name ~data:w ;
      Handlers.on_launch w name parameters >>= fun state ->
      Clock_ns.every
        ~stop:(Ivar.read w.terminating)
        ~continue_on_error:false
        (Time_ns.Span.of_int_sec 60) begin fun () ->
        Time_stamp_counter.Calibrator.calibrate w.calibrator
      end ;
      w.status <- Running (Time_ns.now ()) ;
      w.state <- Some state ;
      don't_wait_for begin
        try_with ~extract_exn:true begin fun () ->
          worker_loop (module Handlers) w
        end >>= function
        | Error (Exit_worker_loop err) ->
          Logger.info (fun m -> m "Worker terminating") >>= fun () ->
          cleanup_worker (module Handlers) w err >>= fun () ->
          Ivar.fill w.terminating () ;
          Deferred.unit
        | _ -> Deferred.unit
      end;
      return w

  let shutdown w =
    may_raise_closed w ;
    let (module Logger) = w.logger in
    Logger.debug (fun m -> m "Triggering shutdown") >>= fun () ->
    trigger_shutdown w ;
    Ivar.read w.terminating

  let state w =
    match w.state, w.status with
    | None, Launching _  ->
      invalid_arg
        (Format.asprintf
           "Worker.state (%s[%a]): \
            state called before worker was initialized"
           base_name Name.pp w.name)
    | None, (Closing _ | Closed _)  ->
      invalid_arg
        (Format.asprintf
           "Worker.state (%s[%a]): \
            state called after worker was terminated"
           base_name Name.pp w.name)
    | None, _  -> assert false
    | Some state, _ -> state

  let latest_events ?(after=Time_ns.min_value) w =
    List.map w.event_log ~f:begin fun (level, ring) ->
      level, Array.filter_map (EventRing.to_array ring)
        ~f:(fun { ts ; evt } -> if ts >= after then Some (ts, evt) else None)
    end

  (* let pending_requests (type a) (w : a queue t) =
   *   let message_queue = match w.buffer with
   *     | Queue_buffer (message_queue, _) -> message_queue
   *     | Bounded_buffer (message_queue, _) -> message_queue in
   *   List.map
   *     ~f:(function (t, Message (req, _)) -> t, Request.view req)
   *     (Pipe.peek message_queue) *)

  let status { status ; _ } = status

  let current_request { current_request ; _ } = current_request

  let view w =
    Types.view (state w) w.parameters

  let list { instances ; _ } =
    Hashtbl.Poly.fold instances
      ~f:(fun ~key:n ~data:w acc -> (n, w) :: acc)
      ~init:[]

  (* let protect { canceler } ?on_error f =
   *   protect ?on_error ~canceler f *)

end
