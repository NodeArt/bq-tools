import * as jose from 'jose';

interface IAccessTokenResponse {
  access_token: string;
  expires_in: number;
  token_type: string;
}

interface IAuthorizationErrorResponse {
  error_description: string;
}

export interface IAccessToken {
  accessToken: string;
  expires: number;
  type: string;
}

export interface ICredentials {
  type: string;
  project_id: string;
  private_key_id: string;
  private_key: string;
  client_email: string;
  client_id: string;
  auth_uri: string;
  token_uri: string;
  auth_provider_x509_cert_url: string;
  client_x509_cert_url: string;
  universe_domain: string;
}

export class AuthService {
  protected readonly cache = new Map<string, IAccessToken>();
  protected cryptoKey?: jose.CryptoKey = undefined;

  constructor(protected readonly credentials: ICredentials) {}

  protected async getCryptoKey() {
    if (!this.cryptoKey) {
      this.cryptoKey = await jose.importPKCS8(this.credentials.private_key, 'RS256', { extractable: false });
    }

    Object.defineProperty(this.cryptoKey, Symbol.toStringTag, { value: 'CryptoKey' });

    return this.cryptoKey;
  }

  protected async issueAuthToken(scopes: string | string[]): Promise<IAccessToken> {
    const credentials = this.credentials;
    const scope = Array.isArray(scopes) ? scopes.join(' ') : scopes;
    const issued = Math.floor(Date.now() / 1000);
    const expires = issued + 3600; // Max 1 hour

    const key = await this.getCryptoKey();

    let jwt: string;
    try {
      jwt = await new jose.SignJWT({ scope })
        .setProtectedHeader({ typ: 'JWT', alg: 'RS256', kid: credentials.private_key_id })
        .setIssuer(credentials.client_email)
        .setSubject(credentials.client_email)
        .setAudience(credentials.token_uri)
        .setExpirationTime(expires)
        .setIssuedAt(issued)
        .sign(key);
    } catch (error) {
      throw new Error(`Failed to sign auth token payload: ${(error as Error)?.message}`);
    }

    const body = new FormData();
    body.append('grant_type', 'urn:ietf:params:oauth:grant-type:jwt-bearer');
    body.append('assertion', `${jwt}`);

    const res = await fetch(credentials.token_uri, { method: 'POST', body });

    if (res.status !== 200) {
      const data = (await res.json()) as IAuthorizationErrorResponse;
      throw new Error(data.error_description);
    }

    const data = (await res.json()) as IAccessTokenResponse;

    return {
      accessToken: data.access_token.replace(/\.+$/, ''),
      type: data.token_type,
      expires,
    };
  }

  public async getAuthToken(scope: string | string[], resetCache = false): Promise<IAccessToken> {
    const cacheKey = Array.isArray(scope) ? [...scope].sort().join(' ') : scope;
    let authToken = this.cache.get(cacheKey);

    if (!authToken || authToken.expires < Math.floor(Date.now() / 1000) + 10 || resetCache) {
      authToken = await this.issueAuthToken(scope);
      this.cache.set(cacheKey, authToken);
    }

    return authToken;
  }

  public static decodeCredentials(value: string): ICredentials {
    return JSON.parse(atob(value));
  }
}
