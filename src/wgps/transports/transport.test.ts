import { delay } from "@std/async";
import { IS_ALFIE, IS_BETTY, type Transport } from "../types.ts";
import { onAsyncIterate } from "../util.ts";
import { transportPairInMemory } from "./in_memory.ts";
import { collectUint8Arrays } from "../../store/storage/payload_drivers/util.ts";
import { TransportWebsocket } from "./websocket.ts";
import { concat } from "@std/bytes";
import { assert, assertEquals } from "@std/assert";

type TestScenarioTransport = {
  name: string;
  makePair: () => Promise<[Transport, Transport]>;
};

const scenarioMemory: TestScenarioTransport = {
  name: "Memory",
  makePair: () => Promise.resolve(transportPairInMemory()),
};

const scenarioWebsocket: TestScenarioTransport = {
  name: "Websocket",
  makePair: async () => {
    const serverSocketPromise = Promise.withResolvers<WebSocket>();

    const server = Deno.serve({
      handler: (req) => {
        const { socket, response } = Deno.upgradeWebSocket(req);

        serverSocketPromise.resolve(socket);

        return response;
      },
      hostname: "0.0.0.0",
      port: 0,
    });

    const clientSocket = new WebSocket(`http://0.0.0.0:${server.addr.port}`);

    const serverSocket = await serverSocketPromise.promise;

    serverSocket.addEventListener("close", () => {
      server.shutdown();
    }, { once: true });

    return [
      new TransportWebsocket(IS_ALFIE, clientSocket),
      new TransportWebsocket(IS_BETTY, serverSocket),
    ];
  },
};

testTransport(scenarioMemory);
testTransport(scenarioWebsocket);

function testTransport(scenario: TestScenarioTransport) {
  Deno.test(`Transport send and receive (${scenario.name})`, async (test) => {
    const [alfie, betty] = await scenario.makePair();

    let receivedAlfie = new Uint8Array(0);

    onAsyncIterate(alfie, (chunk) => {
      receivedAlfie = concat([receivedAlfie, chunk]);
    });

    let receivedBetty = new Uint8Array(0);

    onAsyncIterate(betty, (chunk) => {
      receivedBetty = concat([receivedBetty, chunk]);
    });

    await test.step("send and assert", async () => {
      await alfie.send(new Uint8Array([1, 2, 3, 4]));
      await betty.send(new Uint8Array([255, 254, 253, 252]));
      await alfie.send(new Uint8Array([5, 6]));
      await betty.send(new Uint8Array([251, 250]));
      await alfie.send(new Uint8Array([7]));
      await betty.send(new Uint8Array([249]));

      await delay(5);

      assertEquals(
        receivedAlfie,
        new Uint8Array([255, 254, 253, 252, 251, 250, 249]),
      );
      assertEquals(receivedBetty, new Uint8Array([1, 2, 3, 4, 5, 6, 7]));
    });

    alfie.close();
    betty.close();
  });

  Deno.test(`Transport close (${scenario.name}) `, async (test) => {
    const [alfie, betty] = await scenario.makePair();

    const receivedAlfie = collectUint8Arrays(alfie);
    const receivedBetty = collectUint8Arrays(betty);

    await test.step("send, close, and collect", async () => {
      await alfie.send(new Uint8Array([1, 2, 3, 4]));
      await betty.send(new Uint8Array([255, 254, 253, 252]));

      await delay(0);

      alfie.close();
      betty.close();

      assert(alfie.isClosed);
      assert(betty.isClosed);

      await alfie.send(new Uint8Array([255]));
      await betty.send(new Uint8Array([1]));

      await delay(0);

      assertEquals(
        await receivedAlfie,
        new Uint8Array([255, 254, 253, 252]),
      );
      assertEquals(await receivedBetty, new Uint8Array([1, 2, 3, 4]));
    });

    alfie.close();
    betty.close();
  });
}
