import { mapAttrTypeFromDb, mapAttrTypeToDb, mapColorToInt } from './SQLMappings';
import { PostgresConnection } from './PostgresConnection';
import { Attr, Note, NoteAttr, NoteTag, NotuCache, Space, Tag, parseQuery } from 'notu';
import { buildNotesQuery } from './PostgresQueryBuilder';


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
    
                await connection.run(
                    `CREATE TABLE Attr (
                        id SERIAL NOT NULL PRIMARY KEY,
                        spaceId INT NOT NULL,
                        name VARCHAR(50) NOT NULL,
                        description VARCHAR(256) NOT NULL,
                        type SMALLINT NOT NULL,
                        color INT NULL,
                        FOREIGN KEY (spaceId) REFERENCES Space(id) ON DELETE CASCADE
                    );`
                );
                await connection.run(`CREATE INDEX Attr_spaceId ON Attr(spaceId);`);
    
                await connection.run(
                    `CREATE TABLE NoteAttr (
                        id SERIAL NOT NULL PRIMARY KEY,
                        noteId INT NOT NULL,
                        attrId INT NOT NULL,
                        value VARCHAR(1000) NOT NULL,
                        tagId INT NULL,
                        CONSTRAINT uniqueness UNIQUE NULLS NOT DISTINCT (noteId, attrId, tagId),
                        FOREIGN KEY (noteId) REFERENCES Note(id) ON DELETE CASCADE,
                        FOREIGN KEY (attrId) REFERENCES Attr(id) ON DELETE CASCADE,
                        FOREIGN KEY (tagId) REFERENCES Tag(id) ON DELETE CASCADE
                    );`
                );
                await connection.run(`CREATE INDEX NoteAttr_noteId ON NoteAttr(noteId);`);
                await connection.run(`CREATE INDEX NoteAttr_attrId ON NoteAttr(attrId);`);
                await connection.run(`CREATE INDEX NoteAttr_tagId ON NoteAttr(tagId);`);
            }

            if (!(await connection.run(`SELECT EXISTS (SELECT FROM information_schema.columns WHERE table_name = 'notetag' AND column_name = 'data');`)).rows[0][0]) {
                await connection.run(`ALTER TABLE NoteTag ADD COLUMN data JSONB`);
                await connection.run(`CREATE INDEX NoteTagDataIdx ON NoteTag USING GIN (data);`);
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
                    'INSERT INTO Space (name, version) VALUES ($1, $2) RETURNING id;',
                    space.name, space.version
                )).rows[0][0] as number;
                space.clean();
            }
            else if (space.isDirty) {
                await connection.run(
                    'UPDATE Space SET name = $1, version = $2 WHERE id = $3;',
                    space.name, space.version, space.id
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


    async saveAttr(attr: Attr): Promise<any> {
        if (attr.isClean)
            return Promise.resolve();

        const connection = await this._connectionFactory();
        try {
            if (attr.isNew) {
                attr.id = (await connection.run(
                    'INSERT INTO Attr (spaceId, name, description, type, color) VALUES ($1, $2, $3, $4, $5) RETURNING id;',
                    attr.space.id, attr.name, attr.description, mapAttrTypeToDb(attr.type), mapColorToInt(attr.color)
                )).rows[0][0] as number;
                attr.clean();
            }
            else if (attr.isDirty) {
                await connection.run(
                    'UPDATE Attr SET spaceId = $1, name = $2, description = $3, type = $4, color = $5 WHERE id = $6;',
                    attr.space.id, attr.name, attr.description, mapAttrTypeToDb(attr.type), mapColorToInt(attr.color), attr.id
                );
                attr.clean();
            }
            else if (attr.isDeleted) {
                await connection.run(
                    'DELETE FROM Attr WHERE id = $1;',
                    attr.id
                );
            }
    
            return Promise.resolve(attr.toJSON());
        }
        finally {
            await connection.close();
        }
    }


    async getNotes(query: string, space: number | Space): Promise<Array<any>> {
        if (space instanceof Space)
            space = space.id;

        query = this._prepareQuery(query, space).substring(query.indexOf(' FROM '));

        return await this._getNotesFromQuery(query);
    }

    async getRelatedNotes(tag: Tag | Note | number): Promise<Array<any>> {
        if (tag instanceof Tag)
            tag = tag.id;
        if (tag instanceof Note)
            tag = tag.id;

        const query = `SELECT n.id, n.spaceId, n.text, n.date FROM Note n INNER JOIN NoteTag nt ON nt.noteId = n.id WHERE nt.tagId = ${tag}`;

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
                    tags: [],
                    attrs: []
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
                        data: x[2],
                        attrs: []
                    };
                    const note = notesMap.get(x[0]);
                    note.tags.push(nt);
                });

                const noteAttrsSQL = `SELECT na.noteId, na.attrId, na.tagId, na.value, a.type ` +
                                    `FROM NoteAttr na INNER JOIN Attr a ON na.attrId = a.id ` +
                                    `WHERE noteId IN (${notes.map(n => n.id).join(',')});`;
                (await connection.run(noteAttrsSQL)).rows.map(x => {
                    const na = {
                        state: 'CLEAN',
                        attrId: x[1],
                        tagId: x[2],
                        value: this._convertAttrValueFromDb(mapAttrTypeFromDb(x[4]), x[3])
                    };
                    const note = notesMap.get(x[0]);
                    if (!!na.tagId)
                        note.tags.find(nt => nt.tagId == na.tagId).attrs.push(na);
                    else
                        note.attrs.push(na);
                });
            }

            return notes;
        }
        finally {
            await connection.close();
        }
    }

    async getNoteCount(query: string, space: number | Space): Promise<number> {
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
                    const allActiveNas = note.attrs;
                    const allNasPendingDeletiong  = note.attrsPendingDeletion;
                    for (const nt of note.tags) {
                        allActiveNas.push(...nt.attrs);
                        allNasPendingDeletiong.push(...nt.attrsPendingDeletion);
                    }
                    await this._saveNoteAttrs(note.id, allActiveNas, connection);
                    await this._deleteNoteAttrs(note.id, allNasPendingDeletiong, connection);
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
                'DELETE Tag WHERE id = $1',
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


    private async _saveNoteAttrs(noteId: number, noteAttrs: Array<NoteAttr>, connection: PostgresConnection): Promise<void> {
        const inserts = noteAttrs.filter(x => x.isNew);
        const updates = noteAttrs.filter(x => x.isDirty);

        if (inserts.length > 0) {
            let command = 'INSERT INTO NoteAttr (noteId, attrId, value, tagId) VALUES ' + inserts.map((x, idx) => `($${(idx * 4) + 1}, $${(idx * 4) + 2}, $${(idx * 4) + 3}, $${(idx * 4) + 4})`).join(', ');
            let args = [];
            for (const insert of inserts) {
                args.push(noteId, insert.attr.id, this._convertAttrValueToDb(insert), insert.tag?.id ?? null);
                insert.clean();
            }
            await connection.run(command, ...args);
        }
        for (const update of updates) {
            await connection.run(
                'UPDATE NoteAttr SET value = $1 WHERE noteId = $2 AND attrId = $3 AND COALESCE(tagId, 0) = $4;',
                this._convertAttrValueToDb(update), noteId, update.attr.id, update.tag?.id ?? 0
            );
            update.clean();
        }
    }

    private async _deleteNoteAttrs(noteId: number, noteAttrsForDeletion: Array<NoteAttr>, connection: PostgresConnection): Promise<void> {
        if (noteAttrsForDeletion.length > 0) {
            let command = `DELETE FROM NoteAttr WHERE noteId = $1 AND (${noteAttrsForDeletion.map((x, idx) => `(attrId = $${(idx * 2) + 2} AND COALESCE(tagId, 0) = $${(idx * 2) + 3})`).join(' OR ')})`;
            let args = [noteId];
            for (const del of noteAttrsForDeletion)
                args.push(del.attr.id, del.tag?.id ?? 0);
            await connection.run(command, ...args);
        }
    }


    customJob(name: string, data: any): Promise<any> {
        return Promise.resolve({});
    }



    private _prepareQuery(query: string, spaceId: number): string {
        const parsedQuery = parseQuery(query);
        return buildNotesQuery(parsedQuery, spaceId, this._cache);
    }


    private _convertAttrValueToDb(noteAttr: NoteAttr): any {
        if (noteAttr.attr.isBoolean)
            return noteAttr.value ? 1 : 0;
        if (noteAttr.attr.isDate) {
            const dateValue = new Date(noteAttr.value);
            if (!(noteAttr.value instanceof Date))
                noteAttr.value = dateValue;
            return dateValue.toISOString();
        }
        return noteAttr.value;
    }

    private _convertAttrValueFromDb(attrType: string, value: string) {
        if (attrType == 'BOOLEAN')
            return Number(value) > 0;
        if (attrType == 'DATE')
            return new Date(value);
        if (attrType == 'NUMBER')
            return Number(value);
        return value;
    }
}