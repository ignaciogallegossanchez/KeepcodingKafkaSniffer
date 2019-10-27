package streaming

import sun.misc.BASE64Decoder
import sun.misc.BASE64Encoder
import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec

class AESEncryption(secret: Array[Byte], algorightm: String = "AES") {

  @throws[Exception]
  def encrypt(data: String): String = {
    val key = generateKey
    val c = Cipher.getInstance(algorightm)
    c.init(Cipher.ENCRYPT_MODE, key)
    val encVal = c.doFinal(data.getBytes)
    new BASE64Encoder().encode(encVal)
  }

  @throws[Exception]
  def decrypt(encryptedData: String): String = {
    val key = generateKey
    val c = Cipher.getInstance(algorightm)
    c.init(Cipher.DECRYPT_MODE, key)
    val decodedValue = new BASE64Decoder().decodeBuffer(encryptedData)
    val decValue = c.doFinal(decodedValue)
    new String(decValue)
  }

  /**
   * Generate a new encryption key.
   */
  @throws[Exception]
  private def generateKey = new SecretKeySpec(secret, algorightm)
}
