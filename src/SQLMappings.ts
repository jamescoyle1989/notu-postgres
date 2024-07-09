export function mapAttrTypeToDb(attrType: 'TEXT' | 'NUMBER' | 'BOOLEAN' | 'DATE'): number {
    switch (attrType) {
        case 'TEXT': return 1;
        case 'NUMBER': return 2;
        case 'BOOLEAN': return 3;
        case 'DATE': return 4;
    }
    throw new Error('Unrecognised attribute type');
}

export function mapAttrTypeFromDb(attrType: number): 'TEXT' | 'NUMBER' | 'BOOLEAN' | 'DATE' {
    switch (attrType) {
        case 1: return 'TEXT';
        case 2: return 'NUMBER';
        case 3: return 'BOOLEAN';
        case 4: return 'DATE';
    }
    throw new Error('Unrecognised attribute type');
}

export function mapColorToInt(hex: string): number {
    if (!hex)
        return null;
    if (hex.startsWith('#'))
        hex = hex.substring(1);
    return parseInt(hex, 16);
}

export function mapIntToColor(color: number): string {
    if (color == null)
        return null;
    return '#' + color.toString(16).toUpperCase().padStart(6, '0');
}

export function mapDateToNumber(date: Date): number {
    return Math.round(date.getTime() / 1000);
}

export function mapNumberToDate(sqlDate: number): Date {
    return new Date(sqlDate * 1000);
}