import { PostgresConnection } from './PostgresConnection';
import { mapIntToColor } from './SQLMappings';


export class NotuPostgresCacheFetcher {

    private _connectionFactory: () => Promise<PostgresConnection>;

    constructor(connectionFactory: () => Promise<PostgresConnection>) {
        this._connectionFactory = connectionFactory;
    }


    async getSpacesData(): Promise<Array<any>> {
        const connection = await this._connectionFactory();
        try {
            return (await connection
                .run('SELECT id, name, version, useCommonSpace FROM Space;'))
                .rows.map(x => ({
                    state: 'CLEAN',
                    id: x[0],
                    name: x[1],
                    version: x[2],
                    useCommonSpace: x[3]
                }));
        }
        finally {
            connection.close();
        }
    }


    async getTagsData(): Promise<Array<any>> {
        const connection = await this._connectionFactory();
        try {
            const tags = (await connection
                .run('SELECT n.id, t.name, n.spaceId, t.color, t.isPublic FROM Note n INNER JOIN Tag t ON n.id = t.id;'))
                .rows.map(x => ({
                    state: 'CLEAN',
                    id: x[0],
                    name: x[1],
                    spaceId: x[2],
                    color: mapIntToColor(x[3]),
                    isPublic: x[4],
                    links: []
                }));
            const tagsMap = new Map<number, any>();
            for (const tag of tags)
                tagsMap.set(tag.id, tag);
            (await connection
                .run('SELECT t.id AS fromId, nt.tagId AS toId FROM Tag t INNER JOIN NoteTag nt ON t.id = nt.noteId;'))
                .rows.map(x => tagsMap.get(x[0]).links.push(x[1]));
            return tags;
        }
        finally {
            connection.close();
        }
    }
}