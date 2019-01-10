module S = Actor_s
module Types = Actor_types

open S

module Make
    (Name : NAME) (Event : EVENT) (Request : REQUEST) (Types : TYPES) : S
  with module Name = Name
   and module Event = Event
   and module Request = Request
   and module Types = Types
