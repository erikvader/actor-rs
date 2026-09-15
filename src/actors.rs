pub mod logger;
pub mod process;
pub mod stdin;

// TODO: create an actor that multiplexes, or load balances. It should take another actor that is
// cloneable and spawn it a couple of times. It should then keep track of all of their addresses and
// distribute messages given to the multiplexer to the actor with the most space in its channel.
// Only interrupt should be distributed evenly among them. The exit status could be a vec of
// whatever they have as error. Should they all share some kind of span or group_id to make it clear
// what actors belong to what multiplex group? How to do that? Add the possibility to add yet
// another span when summoning an actor? Should the multiplexer even be the one to summon the
// actors? It's maybe better to just give it a vec of addresses, cuz then the caller can create them
// with arbitrary complexity that a simple clone can't.
