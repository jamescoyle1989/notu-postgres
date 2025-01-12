import { ParsedQuery, ParsedTag, NotuCache, Tag } from 'notu';

export function buildNewNotesQuery(
    parsedQuery: ParsedQuery,
    spaceId: number,
    cache: NotuCache
): string {
    let output = 'SELECT n.id, n.spaceId, n.text, n.date FROM Note n LEFT JOIN Tag t ON n.id = t.id';

    if (!!parsedQuery.where || !!spaceId) {
        let whereClauses: Array<string> = [];
        if (!!spaceId)
            whereClauses.push(`n.spaceId = ${spaceId}`);
        if (!!parsedQuery.where)
            whereClauses.push(`(${buildNewNotesQueryPortion(parsedQuery, spaceId, cache, 'where')})`);
        output += ` WHERE ${whereClauses.join(' AND ')}`
    }
    if (!!parsedQuery.order)
        output += ` ORDER BY ${buildNewNotesQueryPortion(parsedQuery, spaceId, cache, 'order')}`;

    output = processLiterals(output);
    output += ';';
    return output;
}


/**
 * Builds up a portion of the query, either the where section or the order section
 * Will go through each tag it can find in that section, swapping it out for a proper SQL expression
 */
function buildNewNotesQueryPortion(
    parsedQuery: ParsedQuery,
    spaceId: number,
    cache: NotuCache,
    portion: string
): string {
    let output: string = null;
    let tagBuilder: (parsedTag: ParsedTag, tag: Tag) => string;
    if (portion == 'where') {
        output = parsedQuery.where;
        tagBuilder = buildTagFilterCondition;
    }
    else if (portion == 'order') {
        output = parsedQuery.order;
        tagBuilder = buildTagOrderClause;
    }
    else
        throw Error('Invalid portion');

    for (let i = 0; i < parsedQuery.tags.length; i++) {
        if (!output.includes(`{tag${i}}`))
            continue;
        const parsedTag = parsedQuery.tags[i];
        let tag: Tag = null;
        if (!!parsedTag.space) {
            tag = cache.getTagByName(
                parsedTag.name,
                !!parsedTag.space ? cache.getSpaceByName(parsedTag.space).id : spaceId
            );
        }
        else if (!!spaceId)
            tag = cache.getTagByName(parsedTag.name, spaceId);
        else {
            const tags = cache.getTagsByName(parsedTag.name);
            if (tags.length > 1)
                throw Error(`Unable to uniquely identify tag '${parsedTag.name}', please include space name`);
            tag = tags[0];
        }
        output = output.replace(`{tag${i}}`, tagBuilder(parsedTag, tag));
    }

    return output;
}


/**
 * The logic for building up a SQL snippet from a tag for the purposes of filtering
 */
function buildTagFilterCondition(parsedTag: ParsedTag, tag: Tag): string {
    let conditions = [];
    for (const searchDepth of parsedTag.searchDepths) {
        if (searchDepth == 0)
            conditions.push(`n.id = ${tag.id}`);
        else if (searchDepth == 1)
            conditions.push(`EXISTS(SELECT 1 ` +
                `FROM NoteTag nt ` +
                `WHERE nt.noteId = n.id AND nt.tagId = ${tag.id}${buildTagDataWhereExpression(parsedTag, 'nt')})`);
        else if (searchDepth == 2)
            conditions.push(`EXISTS(SELECT 1 ` +
                `FROM NoteTag nt1 INNER JOIN NoteTag nt2 ON nt2.noteId = nt1.tagId ` +
                `WHERE nt1.noteId = n.id AND nt2.tagId = ${tag.id}${buildTagDataWhereExpression(parsedTag, 'nt1')})`);
    }
    let output = conditions.join(' OR ');
    if (conditions.length > 1)
        output = `(${output})`;
    return output;
}


/**
 * The logic for building up a SQL snippet from a tag for the purposes of ordering
 */
function buildTagOrderClause(parsedTag: ParsedTag, tag: Tag): string {
    if (parsedTag.searchDepths.length != 1)
        throw Error('Order clauses must specify exactly one search depth which they are ordering by')
    const searchDepth = parsedTag.searchDepths[0];
    if (searchDepth == 0)
        return `n.id = ${tag.id}`;
    if (searchDepth == 1)
        return `(SELECT ${buildTagDataExpression(parsedTag, 'nt')} ` +
            `FROM NoteTag nt ` +
            `WHERE nt.noteId = n.id AND nt.tagId = ${tag.id})`;
    if (searchDepth == 2)
        return `(SELECT ${buildTagDataExpression(parsedTag, 'nt1')} ` +
            `FROM NoteTag nt1 INNER JOIN NoteTag nt2 ON nt2.noteId = nt1.tagId ` +
            `WHERE nt1.noteId = n.id AND nt2.tagId = ${tag.id}`
}


/**
 * A very light wrapper around buildTagDataExpression. Just makes sure that tag data filtering is AND'ed onto the rest of the filter
 */
function buildTagDataWhereExpression(parsedTag: ParsedTag, noteTagsAlias: string): string {
    let output = buildTagDataExpression(parsedTag, noteTagsAlias);
    if (output != '')
        output = ` AND (${output})`;
    return output;
}


/**
 * Takes in a parsed tag and generates SQL to query the jsonb data attached to it
 */
function buildTagDataExpression(parsedTag: ParsedTag, noteTagsAlias: string): string {
    if (!parsedTag.filter)
        return '';
    let output = parsedTag.filter.pattern;
    for (let i = 0; i < parsedTag.filter.exps.length; i++) {
        const parts = parsedTag.filter.exps[i].split('.').map(x => `'${x}'`);
        let exp = 'data';
        for (let i = 0; i < parts.length; i++) {
            if (i + 1 == parts.length)
                exp += '->>';
            else
                exp += '->';
            exp += parts[i];
        }
        output = output.replace(`{exp${i}}`, `${noteTagsAlias}.${exp}`);
    }
    return output;
}


/**
 * Logic for processing literals added by NotuQL for the purposes of being slightly more cross-platform
 */
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