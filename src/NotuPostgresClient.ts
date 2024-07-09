import { mapAttrTypeFromDb, mapAttrTypeToDb, mapColorToInt, mapDateToNumber, mapNumberToDate } from './SQLMappings';
import { PostgresConnection } from './PostgresConnection';
import { Attr, Note, NoteAttr, NoteTag, NotuCache, Space, Tag, parseQuery } from 'notu';
import { buildNotesQuery } from './PostgresQueryBuilder';


/**
 * Provides methods for common functionality when interacting with the DB
 */
export class NotuPostgresClient {

    private _connectionFactory: () => PostgresConnection;
    
    private _cache: NotuCache;

    constructor(connectionFactory: () => PostgresConnection, cache: NotuCache) {
        this._connectionFactory = connectionFactory;
        this._cache = cache;
    }


    login(username: string, password: string): Promise<string> {
        throw Error('Not implemented.');
    }

    async setup(): Promise<void> {
        const connection = this._connectionFactory();
        try {
            if (!(await connection.run(`SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'Note';`))) {
                await connection.run(
                    `CREATE TABLE Space (
                        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL,
                        version TEXT NOT NULL
                    )`
                );
                
                await connection.run(
                    `CREATE TABLE Note (
                        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                        spaceId INTEGER NOT NULL,
                        text TEXT NOT NULL,
                        date INTEGER NOT NULL,
                        FOREIGN KEY (spaceId) REFERENCES Space(id) ON DELETE CASCADE
                    );`
                );
                await connection.run(`CREATE INDEX Note_spaceId ON Note(spaceId);`);
                await connection.run(`CREATE INDEX Note_date ON Note(date);`);
    
                await connection.run(
                    `CREATE TABLE Tag (
                        id INTEGER NOT NULL,
                        name TEXT NOT NULL,
                        color INTEGER NULL,
                        isPublic INTEGER NOT NULL,
                        PRIMARY KEY (id),
                        FOREIGN KEY (id) REFERENCES Note(id) ON DELETE CASCADE
                    );`
                );
                await connection.run(`CREATE INDEX Tag_id ON Tag(id);`);
    
                await connection.run(
                    `CREATE TABLE NoteTag (
                        noteId INTEGER NOT NULL,
                        tagId INTEGER NOT NULL,
                        PRIMARY KEY (noteId, tagId),
                        FOREIGN KEY (noteId) REFERENCES Note(id) ON DELETE CASCADE,
                        FOREIGN KEY (tagId) REFERENCES Tag(id) ON DELETE CASCADE
                    );`
                );
                await connection.run(`CREATE INDEX NoteTag_noteId ON NoteTag(noteId);`);
                await connection.run(`CREATE INDEX NoteTag_tagId ON NoteTag(tagId);`);
    
                await connection.run(
                    `CREATE TABLE Attr (
                        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                        spaceId INTEGER NOT NULL,
                        name TEXT NOT NULL,
                        description TEXT NOT NULL,
                        type INTEGER NOT NULL,
                        color INTEGER NULL,
                        FOREIGN KEY (spaceId) REFERENCES Space(id) ON DELETE CASCADE
                    );`
                );
                await connection.run(`CREATE INDEX Attr_spaceId ON Attr(spaceId);`);
    
                await connection.run(
                    `CREATE TABLE NoteAttr (
                        noteId INTEGER NOT NULL,
                        attrId INTEGER NOT NULL,
                        value TEXT NOT NULL,
                        tagId INTEGER NULL,
                        PRIMARY KEY (noteId, attrId, tagId),
                        FOREIGN KEY (noteId) REFERENCES Note(id) ON DELETE CASCADE,
                        FOREIGN KEY (attrId) REFERENCES Attr(id) ON DELETE CASCADE,
                        FOREIGN KEY (tagId) REFERENCES Tag(id) ON DELETE CASCADE
                    );`
                );
                await connection.run(`CREATE INDEX NoteAttr_noteId ON NoteAttr(noteId);`);
                await connection.run(`CREATE INDEX NoteAttr_attrId ON NoteAttr(attrId);`);
                await connection.run(`CREATE INDEX NoteAttr_tagId ON NoteAttr(tagId);`);
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

        const connection = this._connectionFactory();
        try {
            if (space.isNew) {
                space.id = await connection.run(
                    'INSERT INTO Space (name, version) VALUES (?, ?) RETURNING id;',
                    space.name, space.version
                ) as number;
                space.clean();
            }
            else if (space.isDirty) {
                await connection.run(
                    'UPDATE Space SET name = ?, version = ? WHERE id = ?;',
                    space.name, space.version, space.id
                );
                space.clean();
            }
            else if (space.isDeleted) {
                await connection.run(
                    'DELETE FROM Space WHERE id = ?;',
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

        const connection = this._connectionFactory();
        try {
            if (attr.isNew) {
                attr.id = await connection.run(
                    'INSERT INTO Attr (spaceId, name, description, type, color) VALUES (?, ?, ?, ?, ?) RETURNING id;',
                    attr.space.id, attr.name, attr.description, mapAttrTypeToDb(attr.type), mapColorToInt(attr.color)
                ) as number;
                attr.clean();
            }
            else if (attr.isDirty) {
                await connection.run(
                    'UPDATE Attr SET spaceId = ?, name = ?, description = ?, type = ?, color = ? WHERE id = ?;',
                    attr.space.id, attr.name, attr.description, mapAttrTypeToDb(attr.type), mapColorToInt(attr.color), attr.id
                );
                attr.clean();
            }
            else if (attr.isDeleted) {
                await connection.run(
                    'DELETE FROM Attr WHERE id = ?;',
                    attr.id
                );
            }
    
            return Promise.resolve(attr.toJSON());
        }
        finally {
            await connection.close();
        }
    }


    getNotes(query: string, space: number | Space): Promise<Array<any>> {
        if (space instanceof Space)
            space = space.id;

        query = this._prepareQuery(query, space).substring(query.indexOf(' FROM '));

        return Promise.resolve(this._getNotesFromQuery(query));
    }

    getRelatedNotes(tag: Tag | Note | number): Promise<Array<any>> {
        if (tag instanceof Tag)
            tag = tag.id;
        if (tag instanceof Note)
            tag = tag.id;

        const query = `SELECT n.id, n.spaceId, n.text, n.date FROM Note n INNER JOIN NoteTag nt ON nt.noteId = n.id WHERE nt.tagId = ${tag}`;

        return Promise.resolve(this._getNotesFromQuery(query));
    }

    private async _getNotesFromQuery(query: string): Promise<Array<any>> {
        const connection = this._connectionFactory();
        try {
            const notesMap = new Map<number, any>();
            const notes = (await connection.run(query)).map(x => {
                const note = {
                    state: 'CLEAN',
                    id: x.id,
                    date: mapNumberToDate(x.date),
                    text: x.text,
                    spaceId: x.spaceId,
                    ownTag: null,
                    tags: [],
                    attrs: []
                };
                notesMap.set(note.id, note);
                return note;
            });
                
            const noteTagsSQL = `SELECT noteId, tagId FROM NoteTag WHERE noteId IN (${notes.map(n => n.id).join(',')});`;
            (await connection.run(noteTagsSQL)).map(x => {
                const nt = {
                    state: 'CLEAN',
                    tagId: x.tagId,
                    attrs: []
                };
                const note = notesMap.get(x.noteId);
                note.tags.push(nt);
            });

            const noteAttrsSQL = `SELECT na.noteId, na.attrId, na.tagId, na.value, a.type ` +
                                `FROM NoteAttr na INNER JOIN Attr a ON na.attrId = a.id ` +
                                `WHERE noteId IN (${notes.map(n => n.id).join(',')});`;
            (await connection.run(noteAttrsSQL)).map(x => {
                const na = {
                    state: 'CLEAN',
                    attrId: x.attrId,
                    tagId: x.tagId,
                    value: this._convertAttrValueFromDb(mapAttrTypeFromDb(x.type), x.value)
                };
                const note = notesMap.get(x.noteId);
                if (!!na.tagId)
                    note.tags.find(x => x.tagId == na.tagId).attrs.push(na);
                else
                    note.attrs.push(na);
            });

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

        const connection = this._connectionFactory();
        try {
            return await connection.run(query)['cnt'];
        }
        finally {
            connection.close();
        }
    }


    async saveNotes(notes: Array<Note>): Promise<Array<any>> {
        const connection = this._connectionFactory();
        try {
            for (const note of notes) {
                if (note.isNew) {
                    note.id = await connection.run(
                        'INSERT INTO Note (date, text, spaceId) VALUES (?, ?, ?) RETURNING id;',
                        mapDateToNumber(note.date), note.text, note.space.id
                    ) as number;
                    note.clean();
                }
                else if (note.isDirty) {
                    await connection.run(
                        'UPDATE Note SET date = ?, text = ?, spaceId = ? WHERE id = ?;',
                        mapDateToNumber(note.date), note.text, note.space.id, note.id
                    );
                    note.clean();
                }
                else if (note.isDeleted) {
                    await connection.run(
                        'DELETE FROM Note WHERE id = ?;',
                        note.id
                    );
                }
                if (!note.isDeleted) {
                    if (!!note.ownTag)
                        this._saveTag(note.ownTag, connection);
                    this._saveNoteTags(note.id, note.tags, connection);
                    this._deleteNoteTags(note.id, note.tagsPendingDeletion, connection);
                    const allActiveNas = note.attrs;
                    const allNasPendingDeletiong  = note.attrsPendingDeletion;
                    for (const nt of note.tags) {
                        allActiveNas.push(...nt.attrs);
                        allNasPendingDeletiong.push(...nt.attrsPendingDeletion);
                    }
                    this._saveNoteAttrs(note.id, allActiveNas, connection);
                    this._deleteNoteAttrs(note.id, allNasPendingDeletiong, connection);
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
                'INSERT INTO Tag (id, name, color, isPublic) VALUES (?, ?, ?, ?);',
                tag.id, tag.name, mapColorToInt(tag.color), tag.isPublic ? 1 : 0
            );
            tag.clean();
        }
        else if (tag.isDirty) {
            await connection.run(
                'UPDATE Tag SET name = ?, color = ?, isPublic = ? WHERE id = ?;',
                tag.name, mapColorToInt(tag.color), tag.isPublic ? 1 : 0, tag.id
            );
            tag.clean();
        }
        else if (tag.isDeleted) {
            await connection.run(
                'DELETE Tag WHERE id = ?',
                tag.id
            );
        }
    }


    private async _saveNoteTags(noteId: number, noteTags: Array<NoteTag>, connection: PostgresConnection): Promise<void> {
        const inserts = noteTags.filter(x => x.isNew);

        if (inserts.length > 0) {
            let command = 'INSERT INTO NoteTag (noteId, tagId) VALUES ' + inserts.map(x => '(?, ?)').join(', ');
            let args = [];
            for (const insert of inserts) {
                args.push(noteId, insert.tag.id);
                insert.clean();
            }
            await connection.run(command, ...args);
        }
    }

    private async _deleteNoteTags(noteId: number, noteTagsPendingDeletion: Array<NoteTag>, connection: PostgresConnection): Promise<void> {
        if (noteTagsPendingDeletion.length > 0) {
            let command = `DELETE FROM NoteTag WHERE noteId = ? AND tagId IN (${noteTagsPendingDeletion.map(x => x.tag.id).join(', ')})`;
            let args = [noteId];
            await connection.run(command, ...args);
        }
    }


    private async _saveNoteAttrs(noteId: number, noteAttrs: Array<NoteAttr>, connection: PostgresConnection): Promise<void> {
        const inserts = noteAttrs.filter(x => x.isNew);
        const updates = noteAttrs.filter(x => x.isDirty);

        if (inserts.length > 0) {
            let command = 'INSERT INTO NoteAttr (noteId, attrId, value, tagId) VALUES ' + inserts.map(x => '(?, ?, ?, ?)').join(', ');
            let args = [];
            for (const insert of inserts) {
                args.push(noteId, insert.attr.id, this._convertAttrValueToDb(insert), insert.tag?.id ?? null);
                insert.clean();
            }
            await connection.run(command, ...args);
        }
        for (const update of updates) {
            await connection.run(
                'UPDATE NoteAttr SET value = ? WHERE noteId = ? AND attrId = ? AND COALESCE(tagId, 0) = ?;',
                this._convertAttrValueToDb(update), noteId, update.attr.id, update.tag?.id ?? 0
            );
            update.clean();
        }
    }

    private async _deleteNoteAttrs(noteId: number, noteAttrsForDeletion: Array<NoteAttr>, connection: PostgresConnection): Promise<void> {
        if (noteAttrsForDeletion.length > 0) {
            let command = `DELETE FROM NoteAttr WHERE noteId = ? AND (${noteAttrsForDeletion.map(x => '(attrId = ? AND COALESCE(tagId, 0) = ?)').join(' OR ')})`;
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
            return mapDateToNumber(dateValue);
        }
        return noteAttr.value;
    }

    private _convertAttrValueFromDb(attrType: string, value: string) {
        if (attrType == 'BOOLEAN')
            return Number(value) > 0;
        if (attrType == 'DATE')
            return mapNumberToDate(Number(value));
        if (attrType == 'NUMBER')
            return Number(value);
        return value;
    }
}