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

module Make (Event : EVENT) (Request : REQUEST) (Types : TYPES) = struct
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

  type message =
      Message: { req: 'a Request.t;
                 resp: 'a Or_error.t Ivar.t option;
                 ts: Time_ns.t; } -> message

  let message ?(ts=Time_ns.now ()) ?resp req =
    Message { req; resp; ts }

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
        message Pipe.Reader.t *
        message Pipe.Writer.t -> infinite queue buffer
    | Bounded_buffer :
        message Pipe.Reader.t *
        message Pipe.Writer.t -> bounded queue buffer
    | Dropbox_buffer :
        message Mvar.Read_write.t -> dropbox buffer

  and 'kind t = {
    limits : Actor_types.limits ;
    timeout : Time_ns.Span.t option ;
    parameters : Types.parameters ;
    mutable (* only for init *) state : Types.state option ;
    monitor : Monitor.t ;
    buffer : 'kind buffer ;
    event_log : (Logs.level * EventRing.t) list ;
    logger : (module Logs_async.LOG) ;
    name : string ;
    full_name : string ;
    id_name : string ;
    id : int ;
    mutable status : Actor_types.worker_status ;
    mutable current_request : current_request option ;
    table : 'kind table ;
    terminating : unit Ivar.t ;
    cleaned : unit Ivar.t ;

    calibrator : Time_stamp_counter.Calibrator.t ;
    mutable prometheus: (Socket.Address.Inet.t, int) Tcp.Server.t option ;
    mutable metrics: (unit -> Prom.t) String.Map.t ;

    mutable nb_events : int ;
    mutable total_treated : int ;
    mutable total_completed : int ;
    mutable total_treated_time : Time_ns.Span.t ;
    mutable total_completed_time : Time_ns.Span.t ;

    quantiles_treated   : Prom.KLL.t ;
    quantiles_completed : Prom.KLL.t ;
  }

  and current_request = {
    pushed : Time_ns.t ;
    treated : Time_ns.t ;
    req : Request.view ;
  }

  and 'kind table = {
    buffer_kind : 'kind buffer_kind ;
    mutable last_id : int ;
    instances : 'kind t String.Table.t ;
    zombies : 'kind t Int.Table.t
  }

  exception Closed of string
  exception Exit_worker_loop of Error.t option

  let summary_of_treated { total_treated; total_treated_time; quantiles_treated; _ } = {
    Prom.count = total_treated ;
    Prom.sum = Time_ns.Span.to_sec total_treated_time;
    data = quantiles_treated, Prom.FSet.of_list [0.5;0.9;0.99]
  }

  let summary_of_completed { total_completed; total_completed_time; quantiles_completed; _ } = {
    Prom.count = total_completed ;
    Prom.sum = Time_ns.Span.to_sec total_completed_time;
    data = quantiles_completed, Prom.FSet.of_list [0.5;0.9;0.99]
  }

  let nb_pending_requests : type a. a t -> int * int = fun w ->
    match w.buffer with
    | Queue_buffer (resps, reqs) -> Pipe.length resps, Pipe.length reqs
    | Bounded_buffer (resps, reqs) -> Pipe.length resps, Pipe.length reqs
    | Dropbox_buffer mv -> if Mvar.is_empty mv then 0, 0 else 1, 1

  let may_raise_closed w =
    if Ivar.is_full w.terminating then raise (Closed w.name)

  let drop_request (w : dropbox t) request =
    may_raise_closed w ;
    let Dropbox { merge } = w.table.buffer_kind in
    let Dropbox_buffer message_box = w.buffer in
    don't_wait_for @@ Monitor.handle_errors begin fun () ->
      match
        match Mvar.peek message_box with
        | None ->
          merge w (Any_request request) None
        | Some (Message { req = old; _ }) ->
          Deferred.(don't_wait_for (ignore_m (Mvar.take message_box))) ;
          merge w (Any_request request) (Some (Any_request old))
      with
      | None -> Deferred.unit
      | Some (Any_request neu) ->
        Mvar.put message_box (message neu)
    end (fun _ -> ())

  let push_request (type a) (w : a queue t) request =
    may_raise_closed w ;
    match w.buffer with
    | Queue_buffer (_, message_queue) ->
      Pipe.write message_queue (message request)
    | Bounded_buffer (_, message_queue) ->
      Pipe.write message_queue (message request)

  let push_request_now (w : infinite queue t) request =
    may_raise_closed w ;
    let Queue_buffer (_, message_queue) = w.buffer in
    Pipe.write_without_pushback message_queue (message request)

  let try_push_request_now (w : bounded queue t) request =
    may_raise_closed w ;
    let Bounded_buffer (_, message_queue) = w.buffer in
    let Bounded { size } = w.table.buffer_kind in
    let qsize = Pipe.length message_queue in
    if qsize < size then begin
      Pipe.write_without_pushback message_queue (message request) ;
      true
    end
    else false

  let push_request_and_wait (type a) (w : a queue t) request =
    may_raise_closed w ;
    let message_queue = match w.buffer with
      | Queue_buffer (_, message_queue) -> message_queue
      | Bounded_buffer (_, message_queue) -> message_queue in
    let resp = Ivar.create () in
    Pipe.write message_queue (message ~resp request) >>= fun () ->
    Ivar.read resp

  let pop_queue w message_queue =
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
      | `Result (`Ok m) -> return (Some m)

  let pop : type a. a t -> message option Deferred.t = fun w ->
    may_raise_closed w ;
    match w.buffer with
    | Queue_buffer (message_queue, _) -> pop_queue w message_queue
    | Bounded_buffer (message_queue, _) -> pop_queue w message_queue
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
    w.nb_events <- Int.succ w.nb_events ;
    may_raise_closed w ;
    let level = Event.level evt in
    if Poly.(level >= w.limits.backlog_level) then
      EventRing.push_back
        (List.Assoc.find_exn ~equal:Poly.(=) w.event_log level)
        (Timestamped_evt.create w.calibrator evt)

  let log_event w evt =
    record_event w evt ;
    let level = Event.level evt in
    let (module Logger) = w.logger in
    Logger.msg level (fun m -> m "@[<v 0>%a@]" Event.pp evt)

  let log_event_now w evt =
    don't_wait_for (log_event w evt)

  module type HANDLERS = sig
    type self
    val on_launch :
      self -> string -> Types.parameters -> Types.state Deferred.t
    val on_launch_complete :
      self -> unit Deferred.t
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
      instances = String.Table.create () ;
      zombies = Int.Table.create () }

  let queue = create_table Queue
  let bounded size = create_table (Bounded { size })
  let dropbox merge = create_table (Dropbox { merge })

  let close (type a) (w : a t) =
    let wakeup = function
      | Message { resp = Some resp; _ } ->
        Ivar.fill resp (Error (Error.of_exn (Closed w.name)))
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
      Option.iter ~f:wakeup (Mvar.peek message_box)

  let cleanup_worker (type kind) handlers (w : kind t) err =
    let (module Logger) = w.logger in
    let (module Handlers : HANDLERS with type self = kind t) = handlers in
    let t0 = match w.status with
      | Running t0 -> t0
      | _ -> assert false in
    w.status <- Closing (t0, Time_ns.now ()) ;
    Logger.debug (fun m -> m "Now in Closing status") >>= fun () ->
    close w ;
    w.status <- Closed (t0, Time_ns.now (), err) ;
    Logger.debug (fun m -> m "Now in Closed status") >>= fun () ->
    String.Table.remove w.table.instances w.name ;
    Handlers.on_close w >>= fun () ->
    w.state <- None ;
    Int.Table.set w.table.zombies ~key:w.id ~data:w ;
    don't_wait_for begin
      Clock_ns.after w.limits.zombie_memory >>= fun () ->
      Logger.debug (fun m -> m "Cleaning up zombie memory") >>= fun () ->
      List.iter ~f:(fun (_, ring) -> EventRing.clear ring) w.event_log ;
      Clock_ns.after Time_ns.Span.(w.limits.zombie_lifetime - w.limits.zombie_memory) >>= fun () ->
      Logger.debug (fun m -> m "Removing zombie from table") >>= fun () ->
      Int.Table.remove w.table.zombies w.id ;
      Ivar.fill w.cleaned () ;
      Logger.debug (fun m -> m "Zombies cleaned")
    end ;
    Logger.debug (fun m -> m "Worker cleaned")

  let process_one (type kind) handlers w =
    let (module Handlers : HANDLERS with type self = kind t) = handlers in
    let (module Logger) = w.logger in
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
    | Some Message {req; resp; ts = pushed } ->
      let treated = Time_ns.now () in
      let current_request = Request.view req in
      w.current_request <- Some { pushed; treated; req = current_request } ;
      w.total_treated <- succ w.total_treated ;
      let treated_time = Time_ns.diff treated pushed in
      w.total_treated_time <- Time_ns.Span.(w.total_treated_time + treated_time) ;
      Prom.KLL.update w.quantiles_treated (Time_ns.Span.to_sec treated_time) ;
      let level = Request.level current_request in
      Logger.msg level begin fun m ->
        m "Request %a" Request.pp current_request
      end >>= fun () ->
      Handlers.on_request w req >>= fun res ->
      Option.iter resp ~f:(fun resp -> Ivar.fill resp (Ok res)) ;
      let completed = Time_ns.now () in
      Handlers.on_completion w req res
        Actor_types.{ pushed ; treated ; completed } >>= fun () ->
      Logger.msg level begin fun m ->
        m "Request %a executed" Request.pp current_request
      end >>| fun () ->
      w.current_request <- None ;
      w.total_completed <- succ w.total_completed ;
      let completed_time = Time_ns.diff completed pushed in
      w.total_completed_time <- Time_ns.Span.(w.total_completed_time + completed_time) ;
      Prom.KLL.update w.quantiles_completed (Time_ns.Span.to_sec completed_time)

  let request_handler w stop _saddr reqd =
    let open Prom in
    let headers =
      Httpaf.Headers.of_list ["Content-Type", "text/plain; version=0.0.4"] in
    let labels = ["hostname", Unix.gethostname (); "actor", w.full_name ] in
    let open Httpaf in
    let resp = Response.create ~headers `OK in
    let nb_resps, nb_reqs = nb_pending_requests w in
    let metrics =
      String.Map.fold_right w.metrics ~init:[] ~f:begin fun ~key:_ ~data a ->
        add_labels labels (data ()) :: a
      end in
    let metrics =
      counter "actor_events_total"
        ~help:"Total number of events since startup."
        [ labels, create_series (Float.of_int w.nb_events) ] ::
      gauge "actor_pending_total"
        ~help:"Current number of pending items."
        [ (("type","request")::labels), create_series (Float.of_int nb_reqs) ;
          (("type","response")::labels), create_series (Float.of_int nb_resps) ] ::
      summary "actor_time_seconds"
        ~help:"Summary of processing time."
        [ (("time","treated")::labels), create_series (summary_of_treated w) ;
          (("time","completed")::labels), create_series (summary_of_completed w) ] ::
      metrics in
    Reqd.respond_with_string
      reqd resp (Format.asprintf "%a" Prom.pp_list metrics) ;
    Ivar.fill stop ()

  let start_prometheus w ~port =
    let default_error_handler stop _saddr ?request:_ error handle =
      let open Httpaf in
      let message =
        match error with
        | `Exn exn -> Exn.to_string exn
        | (#Status.client_error | #Status.server_error) as error ->
          Status.to_string error
      in
      let body = handle Headers.empty in
      Body.write_string body message;
      Body.close_writer body ;
      Ivar.fill stop ()
    in
    let http_handler addr sock =
      let stop = Ivar.create () in
      Deferred.any_unit [
        Ivar.read stop ;
        Httpaf_async.Server.create_connection_handler
          ~request_handler:(request_handler w stop)
          ~error_handler:(default_error_handler stop)
          addr sock
      ] in
    let open Tcp in
    Server.create_sock
      ~on_handler_error:`Ignore
      (Where_to_listen.of_port port)
      http_handler

  let handle_errors (type kind)
      (module Logger : Logs_async.LOG)
      (module Handlers : HANDLERS with type self = kind t)
      (w : kind t) exn =
    let exit_worker_loop err =
      Ivar.fill w.terminating () ;
      Logger.info (fun m -> m "Worker terminating") >>= fun () ->
      Monitor.try_with begin fun () -> begin
          match w.prometheus with
          | None -> Deferred.unit
          | Some p -> Tcp.Server.close ~close_existing_connections:true p
        end >>= fun () ->
        cleanup_worker (module Handlers) w err
      end >>= fun _ ->
      Deferred.unit in
    match Monitor.extract_exn exn with
    | Exit_worker_loop err -> exit_worker_loop err
    | exn ->
      Logger.err (fun m -> m "Exception raised in actor's monitor: %a" Exn.pp exn) >>= fun () ->
      match w.current_request with
      | None -> assert false
      | Some { pushed; treated; req } ->
        let completed = Time_ns.now () in
        w.current_request <- None ;
        Monitor.try_with_or_error begin fun () ->
          Handlers.on_error w req Actor_types.{ pushed ; treated ; completed } (Error.of_exn exn)
        end >>= function
        | Ok () -> Deferred.unit
        | Error e ->
          Logger.err begin fun m ->
            m "@[<v 0>Worker crashed:@,%a@]" Error.pp e
          end >>= fun () ->
          exit_worker_loop (Some e)

  let launch
    : type kind.
      ?log_src:Logs.Src.t ->
      ?timeout:Time_ns.Span.t ->
      ?prom:int ->
      base_name:string list ->
      name:string ->
      kind table ->
      Actor_types.limits ->
      Types.parameters ->
      (module HANDLERS with type self = kind t) ->
      kind t Deferred.t
    = fun ?log_src ?timeout ?prom ~base_name ~name table limits parameters (module Handlers) ->
      let full_name = String.concat ~sep:"." base_name ^ "." ^ name in
      let id =
        table.last_id <- table.last_id + 1 ;
        table.last_id in
      let id_name = Printf.sprintf "%s(%d)" full_name id in
      if String.Table.mem table.instances name then
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
        (val (Logs_async.src_log (Option.value log_src ~default:(Logs.Src.create id_name)))) in
      let w = { limits ;
                parameters ;
                name ;
                full_name ;
                id_name ;
                table ;
                buffer ;
                logger = (module Logger) ;
                state = None ;
                id ;
                monitor = Monitor.create () ;
                event_log ; timeout ;
                current_request = None ;
                status = Launching (Time_ns.now ()) ;
                terminating = Ivar.create () ;
                cleaned  = Ivar.create () ;
                calibrator = Time_stamp_counter.Calibrator.create () ;
                prometheus = None;
                metrics = String.Map.empty ;

                quantiles_treated = Prom.KLL.create () ;
                quantiles_completed = Prom.KLL.create () ;

                total_treated = 0;
                total_completed = 0;
                total_treated_time = Time_ns.Span.zero ;
                total_completed_time = Time_ns.Span.zero ;

                nb_events = 0;
              } in
      begin match prom with
        | None -> return None
        | Some port ->
          start_prometheus w ~port >>| Option.some
      end >>= fun prometheus ->
      w.prometheus <- prometheus ;
      Logger.info (fun m -> m "Worker started for %s" name) >>= fun () ->
      String.Table.set table.instances ~key:name ~data:w ;
      Handlers.on_launch w name parameters >>= fun state ->
      Clock_ns.every
        ~stop:(Ivar.read w.terminating)
        ~continue_on_error:false
        (Time_ns.Span.of_int_sec 60) begin fun () ->
        Time_stamp_counter.Calibrator.calibrate w.calibrator
      end ;
      w.status <- Running (Time_ns.now ()) ;
      w.state <- Some state ;
      let rec inner () =
        try_with (fun () -> process_one (module Handlers) w) >>= function
        | Ok () -> inner ()
        | Error exn ->
          handle_errors (module Logger) (module Handlers) w exn >>=
          inner in
      don't_wait_for (inner ()) ;
      Monitor.detach_and_iter_errors w.monitor ~f:begin fun exn ->
        don't_wait_for (handle_errors (module Logger) (module Handlers) w exn)
      end ;
      Handlers.on_launch_complete w >>= fun () ->
      return w

  let shutdown w =
    may_raise_closed w ;
    let (module Logger) = w.logger in
    Logger.info (fun m -> m "Triggering shutdown") >>= fun () ->
    trigger_shutdown w ;
    Ivar.read w.terminating

  let state w =
    match w.state, w.status with
    | None, Launching _  ->
      invalid_argf "Worker.state (%s): state called before worker was \
                    initialized" w.id_name ()
    | None, (Closing _ | Closed _)  ->
      invalid_argf "Worker.state (%s): state called after worker was \
                    terminated" w.id_name ()
    | None, _  -> assert false
    | Some state, _ -> state

  let latest_events ?(after=Time_ns.epoch) w =
    List.map w.event_log ~f:begin fun (level, ring) ->
      level, Array.filter_map (EventRing.to_array ring) ~f:(fun { ts ; evt } ->
          if Time_ns.is_later ts ~than:after then Some (ts, evt) else None)
    end

  let status { status ; _ } = status

  let current_request { current_request ; _ } = current_request

  let view w =
    Types.view (state w) w.parameters

  let list { instances ; _ } =
    String.Table.fold instances
      ~f:(fun ~key:n ~data:w acc -> (n, w) :: acc)
      ~init:[]

  let set_metrics t m =
    t.metrics <-  List.fold_left
        ~init:String.Map.empty
        ~f:(fun a (key, data) -> String.Map.set a ~key ~data) m

  let merge_metrics t m =
    t.metrics <- List.fold_left
        ~init:t.metrics
        ~f:(fun a (key, data) -> String.Map.set a ~key ~data) m
end
