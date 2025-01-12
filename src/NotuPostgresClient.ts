import { mapColorToInt } from './SQLMappings';
import { PostgresConnection } from './PostgresConnection';
import { Note, NoteTag, NotuCache, Space, Tag, parseQuery } from 'notu';
import { buildNewNotesQuery } from './PostgresQueryBuilder';


/**
 * Provides methods for common functionality when interacting with the DB
 */
export class NotuPostgresClient {

    private _connectionFactory: () => Promise<PostgresConnection>;
    
    private _cache: NotuCache;

    constructor(connectionFactory: () => Promise<PostgresConnection>, cache: NotuCache) {
        this._connectionFactory = connectionFactory;
        this._cache = cache;
    }


    login(username: string, password: string): Promise<string> {
        throw Error('Not implemented.');
    }

    async setup(): Promise<void> {
        const connection = await this._connectionFactory();
        try {
            if (!(await connection.run(`SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'note');`)).rows[0][0]) {
                await connection.run(
                    `CREATE TABLE Space (
                        id SERIAL NOT NULL PRIMARY KEY,
                        name VARCHAR(25) NOT NULL,
                        version VARCHAR(10) NOT NULL
                    )`
                );
                
                await connection.run(
                    `CREATE TABLE Note (
                        id SERIAL NOT NULL PRIMARY KEY,
                        spaceId INT NOT NULL,
                        text TEXT NOT NULL,
                        date TIMESTAMP NOT NULL,
                        FOREIGN KEY (spaceId) REFERENCES Space(id) ON DELETE CASCADE
                    );`
                );
                await connection.run(`CREATE INDEX Note_spaceId ON Note(spaceId);`);
                await connection.run(`CREATE INDEX Note_date ON Note(date);`);
    
                await connection.run(
                    `CREATE TABLE Tag (
                        id INT NOT NULL PRIMARY KEY,
                        name VARCHAR(50) NOT NULL,
                        color INT NULL,
                        isPublic BOOL NOT NULL,
                        FOREIGN KEY (id) REFERENCES Note(id) ON DELETE CASCADE
                    );`
                );
                await connection.run(`CREATE INDEX Tag_id ON Tag(id);`);
    
                await connection.run(
                    `CREATE TABLE NoteTag (
                        noteId INT NOT NULL,
                        tagId INT NOT NULL,
                        PRIMARY KEY (noteId, tagId),
                        FOREIGN KEY (noteId) REFERENCES Note(id) ON DELETE CASCADE,
                        FOREIGN KEY (tagId) REFERENCES Tag(id) ON DELETE CASCADE
                    );`
                );
                await connection.run(`CREATE INDEX NoteTag_noteId ON NoteTag(noteId);`);
                await connection.run(`CREATE INDEX NoteTag_tagId ON NoteTag(tagId);`);
            }

            if (!(await connection.run(`SELECT EXISTS (SELECT FROM information_schema.columns WHERE table_name = 'notetag' AND column_name = 'data');`)).rows[0][0]) {
                await connection.run(`ALTER TABLE NoteTag ADD COLUMN data JSONB;`);
                await connection.run(`CREATE INDEX NoteTagDataIdx ON NoteTag USING GIN (data);`);
            }

            if (!(await connection.run(`SELECT EXISTS (SELECT FROM information_schema.columns WHERE table_name = 'space' AND column_name = 'usecommonspace');`)).rows[0][0]) {
                await connection.run(`ALTER TABLE Space ADD COLUMN useCommonSpace BOOL NOT NULL DEFAULT false;`);
            }

            if (!!(await connection.run(`SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'attr');`)).rows[0][0]) {
                await connection.run(`DROP TABLE NoteAttr;`);
                await connection.run(`DROP TABLE Attr;`);
            }
    
            return Promise.resolve();
        }
        finally {
            await connection.close();
        }
    }


    async saveSpace(space: Space): Promise<any> {
        if (space.isClean)
            return Promise.resolve();

        const connection = await this._connectionFactory();
        try {
            if (space.isNew) {
                space.id = (await connection.run(
                    'INSERT INTO Space (name, version, useCommonSpace) VALUES ($1, $2, $3) RETURNING id;',
                    space.name, space.version, space.useCommonSpace ? 1 : 0
                )).rows[0][0] as number;
                space.clean();
            }
            else if (space.isDirty) {
                await connection.run(
                    'UPDATE Space SET name = $1, version = $2, useCommonSpace = $3 WHERE id = $4;',
                    space.name, space.version, space.useCommonSpace ? 1 : 0, space.id
                );
                space.clean();
            }
            else if (space.isDeleted) {
                await connection.run(
                    'DELETE FROM Space WHERE id = $1;',
                    space.id
                );
            }
    
            return Promise.resolve(space.toJSON());
        }
        finally {
            await connection.close();
        }
    }


    async getNotes(query: string, space?: number | Space): Promise<Array<any>> {
        if (space instanceof Space)
            space = space.id;

        query = this._prepareQuery(query, space);

        return await this._getNotesFromQuery(query);
    }

    private async _getNotesFromQuery(query: string): Promise<Array<any>> {
        const connection = await this._connectionFactory();
        try {
            const notesMap = new Map<number, any>();
            const notes = (await connection.run(query)).rows.map(x => {
                const note = {
                    state: 'CLEAN',
                    id: x[0],
                    date: x[3],
                    text: x[2],
                    spaceId: x[1],
                    ownTag: null,
                    tags: []
                };
                notesMap.set(note.id, note);
                return note;
            });
            
            if (notes.length > 0) {

                const noteTagsSQL = `SELECT noteId, tagId, data FROM NoteTag WHERE noteId IN (${notes.map(n => n.id).join(',')});`;
                (await connection.run(noteTagsSQL)).rows.map(x => {
                    const nt = {
                        state: 'CLEAN',
                        tagId: x[1],
                        data: x[2]
                    };
                    const note = notesMap.get(x[0]);
                    note.tags.push(nt);
                });
            }

            return notes;
        }
        finally {
            await connection.close();
        }
    }

    async getNoteCount(query: string, space?: number | Space): Promise<number> {
        if (space instanceof Space)
            space = space.id;

        query = 'SELECT COUNT(*) AS cnt' + this._prepareQuery(query, space).substring(query.indexOf(' FROM '));

        const connection = await this._connectionFactory();
        try {
            return (await connection.run(query)).rows[0][0];
        }
        finally {
            connection.close();
        }
    }


    async saveNotes(notes: Array<Note>): Promise<Array<any>> {
        const connection = await this._connectionFactory();
        try {
            for (const note of notes) {
                if (note.isNew) {
                    note.id = (await connection.run(
                        'INSERT INTO Note (date, text, spaceId) VALUES ($1, $2, $3) RETURNING id;',
                        note.date, note.text, note.space.id
                    )).rows[0][0] as number;
                    note.clean();
                }
                else if (note.isDirty) {
                    await connection.run(
                        'UPDATE Note SET date = $1, text = $2, spaceId = $3 WHERE id = $4;',
                        note.date, note.text, note.space.id, note.id
                    );
                    note.clean();
                }
                else if (note.isDeleted) {
                    await connection.run(
                        'DELETE FROM Note WHERE id = $1;',
                        note.id
                    );
                }
                if (!note.isDeleted) {
                    if (!!note.ownTag)
                        await this._saveTag(note.ownTag, connection);
                    await this._saveNoteTags(note.id, note.tags, connection);
                    await this._deleteNoteTags(note.id, note.tagsPendingDeletion, connection);
                }
            }

            return Promise.resolve(notes.map(n => n.toJSON()))
        }
        finally {
            await connection.close();
        }
    }


    private async _saveTag(tag: Tag, connection: PostgresConnection): Promise<void> {
        if (tag.isNew) {
            await connection.run(
                'INSERT INTO Tag (id, name, color, isPublic) VALUES ($1, $2, $3, $4);',
                tag.id, tag.name, mapColorToInt(tag.color), tag.isPublic ? 1 : 0
            );
            tag.clean();
        }
        else if (tag.isDirty) {
            await connection.run(
                'UPDATE Tag SET name = $1, color = $2, isPublic = $3 WHERE id = $4;',
                tag.name, mapColorToInt(tag.color), tag.isPublic ? 1 : 0, tag.id
            );
            tag.clean();
        }
        else if (tag.isDeleted) {
            await connection.run(
                'DELETE FROM Tag WHERE id = $1',
                tag.id
            );
        }
    }


    private async _saveNoteTags(noteId: number, noteTags: Array<NoteTag>, connection: PostgresConnection): Promise<void> {
        const inserts = noteTags.filter(x => x.isNew);
        const updates = noteTags.filter(x => x.isDirty);

        if (inserts.length > 0) {
            let command = 'INSERT INTO NoteTag (noteId, tagId, data) VALUES ' + inserts.map((x, idx) => `($${(idx * 3) + 1}, $${(idx * 3) + 2}, $${(idx * 3) + 3})`).join(', ');
            let args = [];
            for (const insert of inserts) {
                args.push(noteId, insert.tag.id, !!insert.data ? JSON.stringify(insert.data) : null);
                insert.clean();
            }
            await connection.run(command, ...args);
        }
        for (const update of updates) {
            await connection.run(
                'UPDATE NoteTag SET data = $1 WHERE noteId = $2 AND tagId = $3;',
                !!update.data ? JSON.stringify(update.data) : null, noteId, update.tag.id
            );
            update.clean();
        }
    }

    private async _deleteNoteTags(noteId: number, noteTagsPendingDeletion: Array<NoteTag>, connection: PostgresConnection): Promise<void> {
        if (noteTagsPendingDeletion.length > 0) {
            let command = `DELETE FROM NoteTag WHERE noteId = $1 AND tagId IN (${noteTagsPendingDeletion.map(x => x.tag.id).join(', ')})`;
            let args = [noteId];
            await connection.run(command, ...args);
        }
    }


    customJob(name: string, data: any): Promise<any> {
        return Promise.resolve({});
    }


    private _prepareQuery(query: string, spaceId?: number): string {
        const parsedQuery = parseQuery(query);
        return buildNewNotesQuery(parsedQuery, spaceId, this._cache);
    }
}