import { Attr, ParsedQuery, NotuCache } from 'notu';

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
        while (output.includes('{Now}'))
            output = output.replace('{Now}', 'NOW()');
    }
    {
        while (output.includes('{Today}'))
            output = output.replace('{Today}', 'CURRENT_DATE');
    }
    {
        while (output.includes('{Yesterday}'))
            output = output.replace('{Yesterday}', `(CURRENT_DATE - INTERVAL'1d')`);
    }
    {
        while (output.includes('{Tomorrow}'))
            output = output.replace('{Tomorrow}', `(CURRENT_DATE + INTERVAL'1d')`);
    }
    {
        while (output.includes('{True}'))
            output = output.replace('{True}', 'TRUE');
    }
    {
        while (output.includes('{False}'))
            output = output.replace('{False}', 'FALSE');
    }

    output += ';';
    return output;
}


function getAttrSQLType(attr: Attr): string {
    if (attr.type == 'TEXT')
        return 'TEXT';
    if (attr.type == 'NUMBER')
        return 'NUMERIC(15, 5)';
    if (attr.type == 'BOOLEAN')
        return 'BOOLEAN';
    if (attr.type == 'DATE')
        return 'TIMESTAMP';
    throw Error('Unexpected attr type');
}