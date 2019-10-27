package streaming

class Notifier {

  /**
   * Method to notify about alert on social network
   * This method is SIMULATED
   * @param message of the notification (message body)
   * @return True if success, false if error while sending notification
   */
  def notify(message: String): Boolean = {
    println(message)
    true
  }

}
