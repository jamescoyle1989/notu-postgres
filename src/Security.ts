import * as jose from 'jose'


export class Security {
    private _userValidator: ((username: string, password: string) => boolean);

    private _secret: Uint8Array;

    private _expirationTime: string;

    constructor(
        userValidator: ((username: string, password: string) => boolean),
        secret: string,
        expirationTime: string = '2h'
    ) {
        this._userValidator = userValidator;
        this._secret = new TextEncoder().encode(secret);
        this._expirationTime = expirationTime;
    }

    async sign(username: string, password: string): Promise<string> {
        if (!this._userValidator(username, password))
            throw Error('Invalid username & password combination');

        return await new jose.SignJWT({username})
            .setProtectedHeader({alg: 'HS256'})
            .setIssuedAt()
            .setExpirationTime(this._expirationTime)
            .sign(this._secret);
    }

    async verify(token: string): Promise<void> {
        try {
            await jose.jwtVerify(token, this._secret);
        }
        catch (err) {
            throw Error('Invalid security token supplied');
        }
    }
}