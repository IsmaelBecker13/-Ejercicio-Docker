#   Becker Ismael - 2025


import asyncio, ssl, certifi, logging, os
import aiomqtt


logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s: %(message)s',
    level=logging.INFO,
    datefmt='%d/%m/%Y %H:%M:%S %z'
)


async def tarea_contador(contador):
    logger = logging.getLogger("contador")
    while True:
        await asyncio.sleep(3)
        contador[0] += 1
        logger.info("Contador incrementado a {}".format(contador[0]))


async def tarea_publicar(client, topico_pub, contador):
    logger = logging.getLogger("publicador")
    while True:
        await asyncio.sleep(5)
        await client.publish(topico_pub, payload=contador[0])
        logger.info("Se ha publicado en {} el mensaje: {}".format(topico_pub, contador[0]))


async def manejar_topico1():
    logger = logging.getLogger("topico 1")
    while True:
        message = await topico1_cola.get()
        logger.info("Manejando: {}, mensaje: {}".format(os.environ['TOPICO1'], message.payload))


async def manejar_topico2():
    logger = logging.getLogger("topico 2")
    while True:
        message = await topico2_cola.get()
        logger.info("Manejando: {}, mensaje: {}".format(os.environ['TOPICO2'], message.payload))


async def distribuidor(client):
    async for message in client.messages:
        if message.topic.matches(os.environ['TOPICO1']):
            topico1_cola.put_nowait(message)
        elif message.topic.matches(os.environ['TOPICO2']):
            topico2_cola.put_nowait(message)


topico1_cola = asyncio.Queue()
topico2_cola = asyncio.Queue()


async def main():
    contador = [0] #variable local como lista (mutable)
    tls_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    tls_context.verify_mode = ssl.CERT_REQUIRED
    tls_context.check_hostname = True
    tls_context.load_default_certs()
    try:
        asyncio.create_task(tarea_contador(contador))
        async with aiomqtt.Client(
            os.environ['SERVIDOR'],
            port=8883,
            tls_context=tls_context,
        ) as client: #uso un solo objeto cliente
            asyncio.create_task(tarea_publicar(client, os.environ['TOPICO_PUB'], contador))
            await client.subscribe(os.environ['TOPICO1'])
            await client.subscribe(os.environ['TOPICO2'])
            async with asyncio.TaskGroup() as tg:
                tg.create_task(distribuidor(client))
                tg.create_task(manejar_topico1())
                tg.create_task(manejar_topico2())
    except KeyboardInterrupt:
        logging.getLogger("excepcion").info("Excepcion capturada - Fin del programa")


if __name__ == "__main__":
    logging.getLogger("inicio").info("Ejercicio Docker - Becker Ismael")
    asyncio.run(main())
