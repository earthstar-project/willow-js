export class WgpsStatusEvent
  extends CustomEvent<{ remaining: number; all: number }> {
  constructor(remaining: number, all: number) {
    super("status", {
      detail: {
        remaining,
        all,
      },
    });
  }
}
