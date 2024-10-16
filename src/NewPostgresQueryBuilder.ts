import { NewParsedQuery, NewParsedTag, NotuCache, Tag } from 'notu';

export function buildNewNotesQuery(
    parsedQuery: NewParsedQuery,
    spaceId: number,
    cache: NotuCache
): string {
    let output = 'SELECT n.id, n.spaceId, n.text, n.date FROM Note n LEFT JOIN Tag t ON n.id = t.id';

    output += ` WHERE n.spaceId = ${spaceId}`;
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
        output = output.replace(`{tag${i}}`, buildTagFilterCondition(parsedTag, tag));
    }

    output = processLiterals(output);
    output += ';';
    return output;
}


function buildTagFilterCondition(parsedTag: NewParsedTag, tag: Tag): string {
    let conditions = [];
    for (const searchDepth of parsedTag.searchDepths) {
        if (searchDepth == 0)
            conditions.push(`n.id = ${tag.id}`);
        else if (searchDepth == 1)
            conditions.push(`EXISTS(SELECT 1 ` +
                `FROM NoteTag nt ` +
                `WHERE nt.noteId = n.id AND nt.tagId = ${tag.id}${buildTagDataFilterExpression(parsedTag, 'nt')})`);
        else if (searchDepth == 2)
            conditions.push(`EXISTS(SELECT 1 ` +
                `FROM NoteTag nt1 INNER JOIN NoteTag nt2 ON nt2.noteId = nt1.tagId ` +
                `WHERE nt1.noteId = n.id AND nt2.tagId = ${tag.id}${buildTagDataFilterExpression(parsedTag, 'nt2')})`);
    }
    let output = conditions.join(' OR ');
    if (conditions.length > 1)
        output = `(${output})`;
    return output;
}


function buildTagDataFilterExpression(parsedTag: NewParsedTag, noteTagsAlias: string): string {
    if (!parsedTag.filter)
        return '';
    let output = parsedTag.filter.pattern;
    for (let i = 0; i < parsedTag.filter.exps.length; i++) {
        let exp = parsedTag.filter.exps[i];
        exp = exp.split('.').map(x => `'${x}'`).join('->');
        output = output.replace(`{exp${i}}`, `${noteTagsAlias}.data->${exp}`);
    }
    return ` AND (${output})`;
}


function processLiterals(query: string) {
    {
        while (query.includes('{Now}'))
            query = query.replace('{Now}', 'NOW()');
    }
    {
        while (query.includes('{Today}'))
            query = query.replace('{Today}', 'CURRENT_DATE');
    }
    {
        while (query.includes('{Yesterday}'))
            query = query.replace('{Yesterday}', `(CURRENT_DATE - INTERVAL'1d')`);
    }
    {
        while (query.includes('{Tomorrow}'))
            query = query.replace('{Tomorrow}', `(CURRENT_DATE + INTERVAL'1d')`);
    }
    {
        while (query.includes('{True}'))
            query = query.replace('{True}', 'TRUE');
    }
    {
        while (query.includes('{False}'))
            query = query.replace('{False}', 'FALSE');
    }
    return query;
}