from Crypto.Cipher import AES
import base64
from utils.Log import Log

BLOCK_SIZE = 16
PADDING = '\0'
pad_it = lambda s: s + (16 - len(s) % 16) * PADDING
# 参考：https://blog.csdn.net/liujingqiu/article/details/79641670
# padding PKCS5的填充方法是根据块的大小默认是16，然后需要加密的明文长度除以16，不足16位字符串的补足到16的倍数，刚好是16的倍数也要补16位，然后补足的不是空格，而是差几位补充几位的Unicode值。
pad = lambda s: s + (BLOCK_SIZE - len(s) % BLOCK_SIZE) * \
                chr(BLOCK_SIZE - len(s) % BLOCK_SIZE)
iv = '128'


# 使用aes算法，进行加密解密操作
# 为跟java实现同样的编码，注意PADDING符号自定义
def encrypt_aes(sourceStr, key):
    if (isinstance(key, str)):
        key = key.encode('utf-8')
    generator = AES.new(key, AES.MODE_ECB)
    crypt = generator.encrypt(pad(sourceStr).encode('utf-8'))
    cryptedStr = base64.b64encode(crypt)
    return cryptedStr.decode()



def decrypt_aes(cryptedStr, key):
    cryptedStr = base64.b64decode(cryptedStr)
    generator = AES.new(key, AES.MODE_ECB)
    recovery = generator.decrypt(cryptedStr)
    decryptedStr = recovery.rstrip(b'\n')
    return decryptedStr.decode()


def main():
    # 2Ofj6+g0OiraaS9bX8hV3A==
    e = encrypt_aes('123456', key=b'5c6f6234fb575b514a1510b8')
    Log.v(e)
    d = decrypt_aes('2Ofj6+g0OiraaS9bX8hV3A==', b'5c6f6234fb575b514a1510b8')
    Log.v(d)


if __name__ == '__main__':
    main()
