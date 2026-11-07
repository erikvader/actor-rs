pub mod adhoc;
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
// What type should the address have? Should it be to the multiplexer that can handle everything
// that the multiplexed actor can handle? Or should it be directly to the multiplexed actor, but it
// is magically forwarded by the multiplexer?
// TODO: create a meta actor that restarts a wrapped actor whenever it crashes?

// TODO: it should be possible to create an actor that represents an actor in another process on
// another computer. It would send messages the other is sending and its Address struct should be
// able to accept everything that the other actor can, so it's pretty much transparent. Is
// Address<Tcp<T>> enough? Or should maybe a Address<T> get returned and maybe an Address<Tcp>, to
// control the connection itself, as well? In any case, i'm pretty sure this would only work if both
// executables are linked to the same library crate that contain all alctors, i don't seee how the
// type checking with Receive trait and such would work otherwise. Over the wire i guess a
// serialized deliverable would need to be sent? I assume that i would probably need some kind of
// unsafe code to promise that an arbitrary chunk of bytes is actually a message that the receiving
// actor can process.

// TODO: but first, create a simple variant of that, which can send and receive strings over a
// datagram unix socket.
