import crypto from 'crypto';
import 'dotenv/config';

export class EncryptionService {
    constructor() {
    }
    
    encryptData(data:string): string {
        let encKey: string = process.env.ENC_KEY;
        let iv: string = encKey.substring(0,10) + encKey.substring(encKey.length-6);

        let cipher = crypto.createCipheriv('aes-256-cbc', encKey, iv);
        let encrypted = cipher.update(data);
        encrypted = Buffer.concat([encrypted, cipher.final()]);

        return encrypted.toString('hex');
    }

    decryptData(encryptedData:string): string {
        let encKey: string = process.env.ENC_KEY;
        let iv: string = encKey.substring(0,10) + encKey.substring(encKey.length-6);

        try {
            if (!encryptedData) return '';
            const encryptedText = Buffer.from(encryptedData, 'hex');
            const decipher = crypto.createDecipheriv('aes-256-cbc', encKey, iv);
            let decrypted = decipher.update(encryptedText);
            decrypted = Buffer.concat([decrypted, decipher.final()]);
            return decrypted.toString();
        }
        catch{
            return ''
        }
    }
}