import shajs from 'sha.js';

export default function signatureGeneration(data: any, secretKey: string) {
    const arrayParams: { key: string; value: any }[] = [];
    Object.entries(data).forEach(([key, value]) => {
      if (!(key == 'signature' || !value))
        arrayParams.push({ key: key, value: value });
    });
    const sorted = arrayParams.sort((a, b) => (a.key > b.key ? 1 : -1));
    const stringToHash =
      sorted.map((x) => x.key + '=' + x.value).join('|') +
      secretKey;
    
    const hash = shajs('sha256').update(stringToHash).digest('hex');
    return hash;
}