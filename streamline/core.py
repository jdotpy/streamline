import asyncio

async def drain(generator):
    """ A no-op drain of a generator """
    items = []
    async for item in generator:
        items.append(item)
    return items

async def transync(synchronous_iter):
    """
        This is a converter which takes a synchronous iterator and yields the items
        asynchronously which allows a standard pipe to convert to an async pipe.
    """
    for item in synchronous_iter:
        yield item

async def static_pipe(stream, inputs):
    source = transync(inputs)
    outputs = await drain(stream(source))
    return outputs

async def pipe(generator, streamers, consumer=None):
    """
        The "pipe" function is the central piece of a stream, the pipe connects
        the generators together in a chain allowing each to pass the result of the
        previous to the next.

        The first generator is special as it does not take an input stream. 

        The last piece of the stream is the consumer which is not a generator but 
        is expected to drain the generator given to it, writing any valued output.
    """
    if consumer is None:
        consumer = drain

    pipe = generator
    for streamer in streamers:
        pipe = streamer(pipe)

    await consumer(pipe)

def sync_exec(future):
    loop = asyncio.get_event_loop()
    task = asyncio.ensure_future(future, loop=loop)
    result = loop.run_until_complete(task)
    return result
