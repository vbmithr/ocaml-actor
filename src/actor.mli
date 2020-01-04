module S = Actor_s
module Types = Actor_types
open S

module Make (Event : EVENT) (Request : REQUEST) (Types : TYPES) : S
  with module Event = Event
   and module Request = Request
   and module Types = Types
