open Core
open Async

open Alcotest
open Alcotest_async

module Event = struct
  type t = unit
  let dummy = ()
  let level () = Logs.Info
  let pp ppf () = Format.pp_print_bool ppf true

  let http_port = None
end

module Request = struct
  type 'a t = unit
  type view = unit
  let view () = ()
  let pp ppf () = Format.pp_print_bool ppf true
end

module Types = struct
  type state = unit
  type parameters = unit
  type view = unit

  let view () () = ()
  let pp ppf () = Format.pp_print_bool ppf true
end

module W = Actor.Make(Event)(Request)(Types)

module Default_handlers = struct
  type self = W.infinite W.queue W.t

  let on_launch_complete _self =
    Deferred.unit
  let on_launch _self _name _params =
    Deferred.unit
  let on_request _ _ =
    Deferred.never ()
  let on_no_request _ =
    Deferred.unit
  let on_close _ =
    Deferred.unit
  let on_error _self _view _status _errors  =
    Deferred.unit
  let on_completion _ () _ _ =
    Deferred.unit
end

let src = Logs.Src.create "ocaml-actor.test"

let set_reporter () =
  let logs =
    Option.Monad_infix.(Sys.getenv "OVH_LOGS_URL" >>| Uri.of_string) in
  let metrics =
    Option.Monad_infix.(Sys.getenv "OVH_METRICS_URL" >>| Uri.of_string) in
  Logs_async_ovh.udp_reporter ?logs ?metrics () >>= fun reporter ->
  Logs.set_reporter reporter ;
  Deferred.unit

let base_name = ["test"]

let worker_init () =
  let table = W.create_table Queue in
  let limits = Actor.Types.create_limits () in
  W.launch table limits ~base_name ~name:"worker_init" () (module Default_handlers) >>= fun w ->
  W.shutdown w

let worker_no_request () =
  let iv = Ivar.create () in
  let module Handlers = struct
    include Default_handlers
    let on_no_request _self =
      Logs_async.debug
        ~src (fun m -> m "Entering 'on_no_request'") >>= fun () ->
      Ivar.fill iv () ;
      Deferred.unit
  end in
  let table = W.create_table Queue in
  let limits = Actor.Types.create_limits () in
  W.launch ~timeout:(Time_ns.Span.of_int_ms 500)
    table limits ~base_name ~name:"worker_no_request" () (module Handlers) >>= fun w ->
  Ivar.read iv >>= fun () ->
  W.shutdown w

let basic =
  "basic", [
    test_case "set_reporter" `Quick set_reporter ;
    test_case "init" `Quick worker_init ;
    test_case "no_request" `Quick worker_no_request ;
  ]

let () =
  Logs.set_level ~all:true (Some Debug) ;
  run "actor" [
    basic
  ]
