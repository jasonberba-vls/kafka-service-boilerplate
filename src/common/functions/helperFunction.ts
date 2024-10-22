import { Request } from 'express';

function getWordsBetweenCurlies(str, SeparatorStart, SeparatorEnd) {
  const regex: string =
    '\\' + SeparatorStart + '([^' + SeparatorEnd + ']+)\\' + SeparatorEnd;
  let results: string[] = [],
    re = new RegExp(regex, 'g'),
    text;

  while ((text = re.exec(str))) {
    results.push(text[1]);
  }
  return results;
}

function getNestedDataValue(obj, keyPath) {
  var lastKeyIndex = keyPath.length - 1;
  for (var i = 0; i < lastKeyIndex; ++i) {
    var key = keyPath[i];
    if (!(key in obj)) {
      obj[key] = {};
    }
    obj = obj[key];
  }
  if (obj[keyPath[lastKeyIndex]] == undefined) {
    obj[keyPath[lastKeyIndex]] = {};
  }
  return obj[keyPath[lastKeyIndex]];
}

function getClientIp(req: Request) {
  const ip = (
    (req.headers['cf-connecting-ip'] as string) ||
    (req.headers['x-forwarded-for'] as string) ||
    ''
  )
    .split(',')[0]
    .trim();

  return ip;
}

function getHostHeader(req: Request) {
  const host = ((req.headers['host'] as string) || '').trim();
  return host;
}

function parseHeaders(headers: any): Record<string, string | undefined> {
  if (headers) {
    return Object.fromEntries(
      Object.entries(headers).map(([key, value]) => [
        key,
        value ? value.toString() : undefined,
      ])
    );
  }
  else
    return null;
}

export {
  getWordsBetweenCurlies,
  getNestedDataValue,
  getClientIp,
  getHostHeader,
  parseHeaders
};
