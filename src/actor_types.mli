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

(** Some memory and time limits. *)
type limits =
  { backlog_size : int ;
  (** Number of event stored in the backlog for each debug level. *)
    backlog_level : Logs.level ;
  (** Stores events at least as important as this value. *)
    zombie_lifetime : Time_ns.Span.t ;
  (** How long dead workers are kept in the introspection table. *)
    zombie_memory : Time_ns.Span.t
  (** How long zombie workers' logs are kept. *) }

val create_limits :
  ?backlog_size:int -> ?backlog_level:Logs.level ->
  ?zombie_lifetime:Time_ns.Span.t -> ?zombie_memory:Time_ns.Span.t ->
  unit -> limits

(** The running status of an individual worker. *)
type worker_status =
  | Launching of Time_ns.t
  | Running of Time_ns.t
  | Closing of Time_ns.t * Time_ns.t
  | Closed of Time_ns.t * Time_ns.t * Error.t option [@@deriving sexp, bin_io]

(** The runnning status of an individual request. *)
type request_status =
  { pushed : Time_ns.t ;
    treated : Time_ns.t ;
    completed : Time_ns.t }

(** The full status of an individual worker. *)
type ('req, 'evt) full_status =
  { status : worker_status ;
    pending_requests : (Time_ns.t * 'req) list ;
    backlog : (Log.Level.t * 'evt list) list ;
    current_request : (Time_ns.t * Time_ns.t * 'req) option }
