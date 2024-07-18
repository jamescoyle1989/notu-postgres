import { Note, Space, Attr, Tag, NotuHttpCacheFetcher } from 'notu';

export function newNote(text?: string, id: number = null): Note {
    const output = new Note(text);
    output.id = id;
    return output;
}

export function newSpace(name?: string, id: number = null): Space {
    const output = new Space(name);
    output.id = id;
    return output;
}

export function newAttr(name?: string, id: number = null): Attr {
    const output = new Attr(name);
    output.id = id;
    return output;
}

export function newTag(name?: string, id: number = null): Tag {
    const output = new Tag(name);
    output.id = id;
    return output;
}


export function testCacheFetcher(): NotuHttpCacheFetcher {
    const spacesData = [
        { id: 1, state: 'CLEAN', name: 'Space 1', version: '1.0.0' },
        { id: 2, state: 'CLEAN', name: 'Space 2', version: '1.0.0' }
    ];
    const tagsData = [
        { id: 1, state: 'CLEAN', name: 'Tag 1', spaceId: 1, color: '#FF0000', isPublic: true, links: [] },
        { id: 2, state: 'CLEAN', name: 'Tag 2', spaceId: 2, color: '#FF0000', isPublic: true, links: [] },
        { id: 3, state: 'CLEAN', name: 'Tag 3', spaceId: 1, color: '#FF0000', isPublic: false, links: [] },
        { id: 4, state: 'CLEAN', name: 'Tag 4', spaceId: 2, color: '#FF0000', isPublic: false, links: [] }
    ];
    const attrsData = [
        { id: 1, state: 'CLEAN', name: 'Attr 1', description: 'Text attr', type: 'TEXT', spaceId: 1 },
        { id: 2, state: 'CLEAN', name: 'Attr 2', description: 'Number attr', type: 'NUMBER', spaceId: 1 },
        { id: 3, state: 'CLEAN', name: 'Attr 3', description: 'Boolean attr', type: 'BOOLEAN', spaceId: 1 },
        { id: 4, state: 'CLEAN', name: 'Attr 4', description: 'Date attr', type: 'DATE', spaceId: 1 }
    ];

    return new NotuHttpCacheFetcher(
        'abc',
        'abc.def.ghi',
        (input: RequestInfo | URL, init?: RequestInit) => {
            let data = [];
            if (input.toString().includes('/spaces'))
                data = spacesData;
            else if (input.toString().includes('/attrs'))
                data = attrsData;
            else if (input.toString().includes('/tags'))
                data = tagsData;

            return Promise.resolve(new Response(JSON.stringify(data), {status: 200}));
        }
    );
}


export class MockConnection {
    history: Array<{type: string, command: string, args: Array<any>}> = [];
    isOpen: boolean = true;

    nextRunOutput: any;

    onRun: (command: string, args: Array<any>) => void;


    async run(command: string, ...args: Array<any>): Promise<any> {
        this.history.push({type: 'run', command, args});
        const output = this.nextRunOutput;
        if (!!this.onRun)
            this.onRun(command, args);
        return output;
    }

    close(): void {
        this.isOpen = false;
    }
}