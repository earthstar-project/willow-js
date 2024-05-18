const EMPTY = Symbol();

export class StaticFIFO<T> {
    private mask: number;
    private top: number;
    private bottom: number;
    public buffer: (T | typeof EMPTY)[];
    public next?: StaticFIFO<T>;

    constructor(capacity: number) {
        if (!(capacity > 0) || ((capacity - 1) & capacity) !== 0) {
            throw new Error("FIFO 'capacity' should be a power of two");
        }

        this.mask = capacity - 1;
        this.top = 0;
        this.bottom = 0;
        this.buffer = Array(capacity).fill(EMPTY);
    }

    public clear() {
        this.top = 0;
        this.bottom = 0;
        this.next = undefined;
        this.buffer.fill(EMPTY);
    }

    public push(value: T) {
        if (this.buffer[this.top] !== EMPTY) {
            return false;
        }

        this.buffer[this.top] = value;
        this.top = (this.top + 1) & this.mask;

        return true;
    }

    public shift() {
        const last = this.buffer[this.bottom];

        if (last === EMPTY) {
            return undefined;
        }

        this.buffer[this.bottom] = EMPTY;
        this.bottom = (this.bottom + 1) & this.mask;

        return last === EMPTY ? undefined : last;
    }

    public peek() {
        const last = this.buffer[this.bottom];
        return last === EMPTY ? undefined : last;
    }
}

export default StaticFIFO;
