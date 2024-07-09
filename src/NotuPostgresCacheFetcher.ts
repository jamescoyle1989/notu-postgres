import { PostgresConnection } from './PostgresConnection';
import { mapAttrTypeFromDb, mapIntToColor } from './SQLMappings';


export class NotuPostgresCacheFetcher {

    private _connectionFactory: () => PostgresConnection;

    constructor(connectionFactory: () => PostgresConnection) {
        this._connectionFactory = connectionFactory;
    }


    async getSpacesData(): Promise<Array<any>> {
        const connection = this._connectionFactory();
        try {
            return (await connection
                .run('SELECT id, name, version FROM Space;'))
                .map(x => ({
                    state: 'CLEAN',
                    id: x.id,
                    name: x.name,
                    version: x.version
                }));
        }
        finally {
            connection.close();
        }
    }


    async getTagsData(): Promise<Array<any>> {
        const connection = this._connectionFactory();
        try {
            const tags = (await connection
                .run('SELECT n.id, t.name, n.spaceId, t.color, t.isPublic FROM Note n INNER JOIN Tag t ON n.id = t.id;'))
                .map(x => ({
                    state: 'CLEAN',
                    id: x.id,
                    name: x.name,
                    spaceId: x.spaceId,
                    color: mapIntToColor(x.color),
                    isPublic: x.isPublic,
                    links: []
                }));
            const tagsMap = new Map<number, any>();
            for (const tag of tags)
                tagsMap.set(tag.id, tag);
            (await connection
                .run('SELECT t.id AS fromId, nt.tagId AS toId FROM Tag t INNER JOIN NoteTag nt ON t.id = nt.noteId;'))
                .map(x => tagsMap.get(x.fromId).links.push(x.toId));
            return Promise.resolve(tags);
        }
        finally {
            connection.close();
        }
    }


    async getAttrsData(): Promise<Array<any>> {
        const connection = this._connectionFactory();
        try {
            return (await connection
                .run('SELECT id, name, description, spaceId, type, color FROM Attr;'))
                .map(x => ({
                    state: 'CLEAN',
                    id: x.id,
                    name: x.name,
                    description: x.description,
                    type: mapAttrTypeFromDb(x.type),
                    spaceId: x.spaceId,
                    color: mapIntToColor(x.color)
                }));
        }
        finally {
            connection.close();
        }
    }
}