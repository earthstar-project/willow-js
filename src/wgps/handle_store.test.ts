import { assertEquals } from "@std/assert";
import { HandleStore } from "./handle_store.ts";

Deno.test("HandleStore.bind", () => {
  const handles = new HandleStore<string>();

  const handleA = handles.bind("a");
  const handleB = handles.bind("b");
  const handleC = handles.bind("c");

  assertEquals(handleA, BigInt(0));
  assertEquals(handleB, BigInt(1));
  assertEquals(handleC, BigInt(2));
});

Deno.test("HandleStore.canUse", () => {
  const handles = new HandleStore<string>();

  const handleA = handles.bind("a");

  assertEquals(handles.canUse(handleA), true);

  handles.markForFreeing(handleA);

  assertEquals(handles.canUse(handleA), false);

  const handleB = handles.bind("b");

  assertEquals(handles.canUse(handleB), true);

  handles.incrementHandleReference(handleB);
  handles.markForFreeing(handleB);

  assertEquals(handles.canUse(handleB), false);
});

Deno.test("HandleStore.markForFreeing", () => {
  const handles = new HandleStore<string>();

  const handleA = handles.bind("a");

  handles.markForFreeing(handleA);

  assertEquals(handles.get(handleA), undefined);

  const handleB = handles.bind("b");

  handles.incrementHandleReference(handleB);
  handles.markForFreeing(handleB);

  assertEquals(handles.get(handleB), "b");
  assertEquals(handles.canUse(handleB), false);
});

Deno.test("HandleStore reference counting", () => {
  const handles = new HandleStore<string>();

  const handle = handles.bind("a");

  handles.incrementHandleReference(handle);
  handles.incrementHandleReference(handle);
  handles.incrementHandleReference(handle);

  handles.markForFreeing(handle);

  assertEquals(handles.get(handle), "a");

  handles.decrementHandleReference(handle);
  assertEquals(handles.get(handle), "a");

  handles.decrementHandleReference(handle);
  assertEquals(handles.get(handle), "a");

  handles.decrementHandleReference(handle);
  assertEquals(handles.get(handle), undefined);
});
