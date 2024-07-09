import { Attr, ParsedQuery, NotuCache } from 'notu';
import { mapDateToNumber } from './SQLMappings';

export function buildNotesQuery(
    parsedQuery: ParsedQuery,
    spaceId: number,
    cache: NotuCache
): string {
    let output = 'SELECT n.id, n.spaceId, n.text, n.date FROM Note n LEFT JOIN Tag t ON n.id = t.id';
    
    output += ` WHERE n.spaceId = ${spaceId}`
    if (!!parsedQuery.where)
        output += ` AND (${parsedQuery.where})`;
    if (!!parsedQuery.order)
        output += ` ORDER BY ${parsedQuery.order}`;

    for (let i = 0; i < parsedQuery.tags.length; i++) {
        const parsedTag = parsedQuery.tags[i];
        const tag = cache.getTagByName(
            parsedTag.name,
            !!parsedTag.space ? cache.getSpaceByName(parsedTag.space).id : spaceId
        );
        if (parsedTag.searchDepth == 0)
            output = output.replace(`{tag${i}}`, `n.id = ${tag.id}`);

        else if (parsedTag.searchDepth == 1 && parsedTag.strictSearchDepth && !parsedTag.includeOwner)
            output = output.replace(`{tag${i}}`, `EXISTS(SELECT 1 FROM NoteTag nt WHERE nt.noteId = n.id AND nt.tagId = ${tag.id})`);

        else if (parsedTag.searchDepth == 1)
            output = output.replace(`{tag${i}}`, `(n.id = ${tag.id} OR EXISTS(SELECT 1 FROM NoteTag nt WHERE nt.noteId = n.id AND nt.tagId = ${tag.id}))`);

        else if (parsedTag.searchDepth == 2 && parsedTag.strictSearchDepth && !parsedTag.includeOwner)
            output = output.replace(`{tag${i}}`, `EXISTS(SELECT 1 FROM NoteTag nt1 INNER JOIN NoteTag nt2 ON nt2.noteId = nt1.tagId WHERE nt1.noteId = n.id AND nt2.tagId = ${tag.id})`);

        else if (parsedTag.searchDepth == 2 && parsedTag.strictSearchDepth && parsedTag.includeOwner)
            output = output.replace(`{tag${i}}`, `(n.id = ${tag.id} OR EXISTS(SELECT 1 FROM NoteTag nt1 INNER JOIN NoteTag nt2 ON nt2.noteId = nt1.tagId WHERE nt1.noteId = n.id AND nt2.tagId = ${tag.id}))`);

        else if (parsedTag.searchDepth == 2 && !parsedTag.strictSearchDepth && !parsedTag.includeOwner)
            output = output.replace(`{tag${i}}`, `EXISTS(SELECT 1 FROM NoteTag nt1 LEFT JOIN NoteTag nt2 ON nt2.noteId = nt1.tagId WHERE nt1.noteId = n.id AND ${tag.id} IN (nt1.tagId, nt2.tagId))`);

        else if (parsedTag.searchDepth == 2)
            output = output.replace(`{tag${i}}`, `(n.id = ${tag.id} OR EXISTS(SELECT 1 FROM NoteTag nt1 LEFT JOIN NoteTag nt2 ON nt2.noteId = nt1.tagId WHERE nt1.noteId = n.id AND ${tag.id} IN (nt1.tagId, nt2.tagId)))`);

        else
            throw Error(`Sorry, that tag search feature hasn't been implemented yet.`);
    }

    for (let i = 0; i < parsedQuery.attrs.length; i++) {
        const parsedAttr = parsedQuery.attrs[i];
        const attr = cache.getAttrByName(parsedAttr.name, spaceId);
        let tagIds = [];
        if (!!parsedAttr.tagNameFilters) {
            tagIds = parsedAttr.tagNameFilters
                .map(parsedTag => cache.getTagByName(
                    parsedTag.name,
                    !!parsedTag.space ? cache.getSpaceByName(parsedTag.space).id : spaceId
                ).id);
        }
        
        if (parsedAttr.exists) {
            if (tagIds.length == 0)
                output = output.replace(`{attr${i}}`, `EXISTS(SELECT 1 FROM NoteAttr na WHERE na.noteId = n.id AND na.attrId = ${attr.id})`);
            else
                output = output.replace(`{attr${i}}`, `EXISTS(SELECT 1 FROM NoteAttr na WHERE na.noteId = n.id AND na.attrId = ${attr.id} AND na.tagId IN (${tagIds.join(',')}))`);
        }
        else {
            if (tagIds.length == 0)
                output = output.replace(`{attr${i}}`, `CAST((SELECT na.value FROM NoteAttr na WHERE na.noteId = n.id AND na.attrId = ${attr.id}) AS ${getAttrSQLType(attr)})`);
            else
                output = output.replace(`{attr${i}}`, `CAST((SELECT na.value FROM NoteAttr na WHERE na.noteId = n.id AND na.attrId = ${attr.id} AND na.tagId IN (${tagIds.join(',')})) AS ${getAttrSQLType(attr)})`);
        }
    }

    //Start of handling special expressions/literals which can be used in queries
    {
        const now = new Date();
        const dateNumber = mapDateToNumber(now).toString();
        while (output.includes('{Now}'))
            output = output.replace('{Now}', dateNumber);
    }
    {
        const today = new Date();
        today.setUTCHours(0, 0, 0, 0);
        const dateNumber = mapDateToNumber(today).toString();
        while (output.includes('{Today}'))
            output = output.replace('{Today}', dateNumber);
    }
    {
        const yesterday = new Date();
        yesterday.setUTCHours(0, 0, 0, 0);
        yesterday.setDate(yesterday.getDate() - 1);
        const dateNumber = mapDateToNumber(yesterday).toString();
        while (output.includes('{Yesterday}'))
            output = output.replace('{Yesterday}', dateNumber);
    }
    {
        const tomorrow = new Date();
        tomorrow.setUTCHours(0, 0, 0, 0);
        tomorrow.setDate(tomorrow.getDate() + 1);
        const dateNumber = mapDateToNumber(tomorrow).toString();
        while (output.includes('{Tomorrow}'))
            output = output.replace('{Tomorrow}', dateNumber);
    }
    //Handle timespan values like {1d 5:00} to express 1 day and 5 hours. Used like {Now} + {0d 3:00}
    {
        const regex = /\{(?:(\d+)d)(?:\s(\d+):(\d{2})(?::(\d{2}))?)?\}/;
        while (true) {
            const match = regex.exec(output);
            if (!match)
                break;
            let secondsTimespan = 24 * 60 * 60 * Number(match[1]);
            if (match[2] != undefined) {
                secondsTimespan += 60 * 60 * Number(match[2]);
                secondsTimespan += 60 * Number(match[3]);
                if (match[4] != undefined)
                    secondsTimespan += Number(match[4]);
            }
            const matchStart = output.indexOf(match[0]);
            const matchEnd = matchStart + match[0].length;
            output = output.substring(0, matchStart) + secondsTimespan.toString() + output.substring(matchEnd);
        }
    }
    //Handle date literals like {2024/06/09 23:50:00}
    {
        const regex = /\{(\d{1,4})(?:\/|-)(\d{1,2})(?:\/|-)(\d{1,2})(?:\s(\d{1,2}):(\d{2})(?::(\d{2}))?)?\}/;
        while (true) {
            const match = regex.exec(output);
            if (!match)
                break;
            const year = Number(match[1]);
            const month = Number(match[2]);
            const day = Number(match[3]);
            let hours = 0;
            let minutes = 0;
            let seconds = 0;
            if (match[4] != undefined) {
                hours = Number(match[4]);
                minutes = Number(match[5]);
                if (match[6] != undefined)
                    seconds = Number(match[6]);
            }
            const matchStart = output.indexOf(match[0]);
            const matchEnd = matchStart + match[0].length;
            const date = new Date(year, month - 1, day, hours, minutes, seconds);
            const dateStr = mapDateToNumber(date).toString();
            output = output.substring(0, matchStart) + dateStr + output.substring(matchEnd);
        }
    }

    output += ';';
    return output;
}


function getAttrSQLType(attr: Attr): string {
    if (attr.type == 'TEXT')
        return 'TEXT';
    if (attr.type == 'NUMBER')
        return 'NUMERIC';
    return 'INTEGER';
}