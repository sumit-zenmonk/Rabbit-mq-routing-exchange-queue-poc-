export interface PublishHeadersInterface {
    'x-match'?: 'all' | 'any';
    [key: string]: any;
}

export type ExchangeType = | 'direct' | 'fanout' | 'topic' | 'headers';