/**
 * Common interface for interacting with Postgres DB through various different clients
 */
export interface PostgresConnection {
    run(command: string, ...args: Array<any>): Promise<any>;

    close(): Promise<void>;
}



/**
 * Provides thin wrapper around pg.Client
 */
export class PgConnection implements PostgresConnection {
    private _internal: any;

    constructor(dbClient: any) {
        this._internal = dbClient;
    }

    async run(command: string, ...args: Array<any>): Promise<any> {
        return await this._internal.query({
            text: command,
            values: args,
            rowMode: 'array'
        });
    }

    async close(): Promise<void> {
        await this._internal.end();
    }
}