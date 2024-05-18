import StaticFIFO from "./static.ts";

export class FIFO<T> {
    private head: StaticFIFO<T>;
    private tail: StaticFIFO<T>;
    private resolve?: (value: T) => void;
    public length: number;

    constructor(capacity: number = 16) {
        this.head = new StaticFIFO<T>(capacity);
        this.tail = this.head;
        this.length = 0;
    }

    public clear() {
        this.head = this.tail;
        this.head.clear();
        this.length = 0;
    }

    public push(value: T) {
        if (this.resolve) {
            this.resolve(value);
            this.resolve = undefined;
            return;
        }

        ++this.length;

        if (!this.head.push(value)) {
            const prev = this.head;
            this.head = new StaticFIFO<T>(2 * prev.buffer.length);
            prev.next = this.head;
            this.head.push(value);
        }
    }

    public shift() {
        if (this.length !== 0) {
            --this.length;
        }

        const value = this.tail.shift();

        if (value === undefined && this.tail.next) {
            const next = this.tail.next;
            this.tail.next = undefined;
            this.tail = next;

            return this.tail.shift();
        }

        return value;
    }

    public peek() {
        const value = this.tail.peek();

        if (value === undefined && this.tail.next) {
            return this.tail.next.peek();
        }

        return value;
    }

    public *[Symbol.iterator]() {
        while (this.length) {
            yield this.shift()!;
        }
    }

    public async *[Symbol.asyncIterator]() {
        while (true) {
            if (this.length) {
                yield this.shift()!;
                continue;
            }

            yield await new Promise<T>(resolve => this.resolve = resolve);
        }
    }
}

export default FIFO;
