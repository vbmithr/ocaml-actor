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

open Core_kernel
open Async_kernel

(** Async based local event loops with automated introspection *)

(** {2 Parameters to build a worker group} *)

(** The name of the group of workers corresponding to an instanciation
    of {!Make}, as well as the name of each worker in that group. *)
module type NAME = sig

  (** The name/path of the worker group *)
  val base : string list

  (** The abstract name of a single worker *)
  type t

  (** Pretty printer for displaying the worker name *)
  val pp : Format.formatter -> t -> unit
  val to_string : t -> string

end

(** Events that are used for logging and introspection.
    Events are pretty printed immediately in the log, and stored in
    the worker's event backlog for introspection. *)
module type EVENT = sig

  (** The type of an event. *)
  type t

  (** A dummy event value to use as placeholder. *)
  val dummy : t

  (** Assigns a logging level to each event.
      Events can be ignored for logging w.r.t. the global node configuration.
      Events can be ignored for introspection w.r.t. to the worker's
      {!Worker_types.limits}. *)
  val level : t -> Logs.level

  (** Pretty printer, also used for logging *)
  val pp : Format.formatter -> t -> unit

  (** Optionally set a target warp10 server URL *)
  val warp10_url : Uri.t option

  (** Optionally set a port for HTTP monitoring server *)
  val http_port : int option

  (** Optionally convert the event to Warp10 format. *)
  val to_warp10 : t -> Warp10.t option

end

(** The type of messages that are fed to the worker's event loop. *)
module type REQUEST = sig

  (** The type of events.
      It is possible to wait for an event to be processed from outside
      the worker using {!push_request_and_wait}. In this case, the
      handler for this event can return a value. The parameter is the
      type of this value. *)
  type 'a t

  (** As requests can contain arbitrary data that may not be
      serializable and are polymorphic, this view type is a
      monomorphic projection sufficient for introspection. *)
  type view

  (** The projection function from full request to simple views. *)
  val view : 'a t -> view

  (** Pretty printer, also used for logging by {!Request_event}. *)
  val pp : Format.formatter -> view -> unit

end

(** The (imperative) state of the event loop. *)
module type TYPES = sig

  (** The internal state that is passed to the event handlers. *)
  type state

  (** The parameters provided when launching a new worker. *)
  type parameters

  (** A simplified view of the worker's state for introspection. *)
  type view

  (** The projection function from full state to simple views. *)
  val view : state -> parameters -> view

  (** Pretty printer for introspection. *)
  val pp : Format.formatter -> view -> unit

end

(** {2 Worker group maker} *)

(** Functor to build a group of workers.
    At that point, all the types are fixed and introspectable,
    but the actual parameters and event handlers can be tweaked
    for each individual worker. *)
module type S = sig

  module Name: NAME
  module Event: EVENT
  module Request: REQUEST
  module Types: TYPES

  (** A handle to a specific worker, parameterized by the type of
      internal message buffer. *)
  type 'kind t

  (** A handle to a table of workers. *)
  type 'kind table

  (** Internal buffer kinds used as parameters to {!t}. *)
  type 'a queue and bounded and infinite
  type dropbox


  (** Supported kinds of internal buffers. *)
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

  (** Create a table of workers. *)
  val create_table : 'kind buffer_kind -> 'kind table

  val queue : infinite queue table
  val bounded : int -> bounded queue table
  val dropbox : (dropbox t ->
                 any_request ->
                 any_request option ->
                 any_request option) -> dropbox table

  (** An error returned when trying to communicate with a worker that
      has been closed. *)
  exception Closed of Name.t

  (** The callback handlers specific to each worker instance. *)
  module type HANDLERS = sig

    (** Placeholder replaced with {!t} with the right parameters
        provided by the type of buffer chosen at {!launch}.*)
    type self

    (** Builds the initial internal state of a worker at launch.
        It is possible to initialize the message queue.
        Of course calling {!state} will fail at that point. *)
    val on_launch :
      self -> Name.t -> Types.parameters -> Types.state Deferred.t

    (** The main request processor, i.e. the body of the event loop. *)
    val on_request :
      self -> 'a Request.t -> 'a Deferred.t

    (** Called when no request has been made before the timeout, if
        the parameter has been passed to {!launch}. *)
    val on_no_request :
      self -> unit Deferred.t

    (** A function called when terminating a worker. *)
    val on_close :
      self -> unit Deferred.t

    (** A function called at the end of the worker loop in case of an
        abnormal error. This function can handle the error by
        returning [Ok ()], or leave the default unexpected error
        behaviour by returning its parameter. A possibility is to
        handle the error for ad-hoc logging, and still use
        {!trigger_shutdown} to kill the worker. *)
    val on_error :
      self ->
      Request.view ->
      Actor_types.request_status ->
      Error.t -> unit Deferred.t

    (** A function called at the end of the worker loop in case of a
        successful treatment of the current request. *)
    val on_completion :
      self ->
      'a Request.t -> 'a ->
      Actor_types.request_status ->
      unit Deferred.t

  end

  (** Creates a new worker instance.
      Parameter [queue_size] not passed means unlimited queue. *)
  val launch :
    'kind table ->
    ?timeout:Time_ns.Span.t ->
    ?http_port:int ->
    Actor_types.limits -> Name.t -> Types.parameters ->
    (module HANDLERS with type self = 'kind t) ->
    'kind t Deferred.t

  (** Triggers a worker termination and waits for its completion.
      Cannot be called from within the handlers.  *)
  val shutdown :
    _ t -> unit Deferred.t

  (** Adds a message to the queue and waits for its result.
      Cannot be called from within the handlers. *)
  val push_request_and_wait :
    _ queue t -> 'a Request.t -> 'a Deferred.Or_error.t

  (** Adds a message to the queue. *)
  val push_request :
    _ queue t -> 'a Request.t -> unit Deferred.t

  (** Adds a message to the queue immediately.
      Returns [false] if the queue is full. *)
  val try_push_request_now :
    bounded queue t -> 'a Request.t -> bool

  (** Adds a message to the queue immediately. *)
  val push_request_now :
    infinite queue t -> 'a Request.t -> unit

  (** Sets the current request. *)
  val drop_request :
    dropbox t -> 'a Request.t -> unit

  (** Gets the monitor associated with the worker. *)
  val monitor : _ t -> Monitor.t

  (* (\** Detects cancelation from within the request handler to stop
   *     asynchronous operations. *\)
   * val protect :
   *   _ t ->
   *   ?on_error: (Error.t list -> 'b Deferred.Or_error.t) ->
   *   (unit -> 'b Deferred.Or_error.t) ->
   *   'b Deferred.Or_error.t *)

  (** Trigger a worker termination. *)
  val trigger_shutdown : _ t -> unit

  (** Record an event in the backlog. *)
  val record_event : _ t -> Event.t -> unit

  (** Record an event and make sure it is logged. *)
  val log_event : _ t -> Event.t -> unit Deferred.t

  (** Record an event and make sure it is logged, but don't wait for
      it. *)
  val log_event_now : _ t -> Event.t -> unit

  (** Access the internal state, once initialized. *)
  val state : _ t -> Types.state

  (** Access the event backlog. *)
  val last_events : _ t -> (Logs.level * Event.t list) list

  (* (\** Introspect the message queue, gives the times requests were pushed. *\)
   * val pending_requests : _ queue t -> (Time_ns.t * Request.view) list *)

  (** Get the running status of a worker. *)
  val status : _ t -> Actor_types.worker_status

  (** Get the request being treated by a worker.
      Gives the time the request was pushed, and the time its
      treatment started. *)
  val current_request : _ t -> (Time_ns.t * Time_ns.t * Request.view) option

  (** Introspect the state of a worker. *)
  val view : _ t -> Types.view

  (** List the running workers in this group.
      After they are killed, workers are kept in the table
      for a number of seconds given in the {!Actor_types.limits}. *)
  val list : 'a table -> (Name.t * 'a t) list
end
