package com.databricks.genericPipelineOrchestrator.utility

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils

import java.security.MessageDigest
import java.util
import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec

object Crypt {

  val scope: String = "test-databricks-scope"
  val secretekey = "decrypt-encrypt-test-key"
  val saltkey = "decrypt-encrypt-test-salt-key"

  def encrypt(key: String, saltKey: String, value: String): String = {
    val cipher: Cipher = Cipher.getInstance("AES/ECB/PKCS5Padding")
    cipher.init(Cipher.ENCRYPT_MODE, keyToSpec(key,saltKey))
    org.apache.commons.codec.binary.Base64.encodeBase64String(cipher.doFinal(value.getBytes("UTF-8")))
  }
  def decrypt(key: String, saltKey: String, encryptedValue: String): String = {
    val cipher: Cipher = Cipher.getInstance("AES/ECB/PKCS5PADDING")
    cipher.init(Cipher.DECRYPT_MODE, keyToSpec(key,saltKey))
    new String(cipher.doFinal(org.apache.commons.codec.binary.Base64.decodeBase64(encryptedValue)))
  }
  def keyToSpec(key: String, saltKey: String): SecretKeySpec = {
    var keyBytes: Array[Byte] = (saltKey + key).getBytes("UTF-8")
    val sha: MessageDigest = MessageDigest.getInstance("SHA-1")
    keyBytes = sha.digest(keyBytes)
    keyBytes = util.Arrays.copyOf(keyBytes, 16)
    new SecretKeySpec(keyBytes, "AES")
  }

}
