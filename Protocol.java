/*
 * Replace the following string of 0s with your student number
 * 230266960
 */

import java.io.File;
import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.io.FileInputStream;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.net.SocketTimeoutException; // This import is necessary for the SocketTimeoutException


public class Protocol {

    static final String  NORMAL_MODE="nm"   ; // normal transfer mode: (for Part 1 and 2)
    static final String	 TIMEOUT_MODE ="wt"  ; // timeout transfer mode: (for Part 3)
    static final String	 GBN_MODE ="gbn"  ;    // GBN transfer mode: (for Part 4)
    static final int DEFAULT_TIMEOUT =10  ;     // default timeout in seconds (for Part 3)
    static final int DEFAULT_RETRIES =4  ;    // default number of consecutive retries (for Part 3)

    /*
     * The following attributes control the execution of a transfer protocol and provide access to the
     * resources needed for a file transfer (such as the file to transfer, etc.)
     *
     */

    private InetAddress ipAddress;      // the address of the server to transfer the file to. This should be a well-formed IP address.
    private int portNumber; 		    // the  port the server is listening on
    private DatagramSocket socket;     // The socket that the client bind to
    private String mode;               //mode of transfer normal/with timeout/GBN

    private File inputFile;           // The client-side input file to transfer
    private String inputFileName;      // the name of the client-side input file for transfer to the server
    private String outputFileName ;    //the name of the output file to create on the server as a result of the file transfer
    private long fileSize;            // the size of the client-side input file

    private Segment dataSeg   ;         // the protocol data segment for sending segments with payload read from the input file to the server
    private Segment ackSeg  ;           //the protocol ack segment for receiving ACKs from the server
    private int maxPayload;				//The max payload size of the data segment
    private long remainingBytes;       //the number of bytes remaining to be transferred during execution of a transfer. This is set to the input file size at the start

    private int timeout;          //the timeout in seconds to use for the protocol with timeout (for Part 3)
    private int maxRetries;       //the maximum number of consecutive retries (retransmissions) to allow before exiting the client (for Part 3)(This is per segment)

    private int sentBytes;       //the accumulated total bytes transferred to the server as the result of a file transfer
    private float lossProb;      //the probability of corruption of a data segment during the transfer  (for Part 3)
    private int currRetry;       //the current number of consecutive retries (retransmissions) following a segment corruption (for Part 3)(This is per segment)
    private int totalSegments;   //the accumulated total number of ALL data segments transferred to the server as the result of a file transfer
    private int resentSegments;  //the accumulated total number of data segments resent to the server as a result of timeouts during a file transfer (for Part 3)

    /**************************************************************************************************************************************
     **************************************************************************************************************************************
     * For this assignment, you have to implement the following methods:
     *		sendMetadata()
     *      readData()
     *      sendData()
     *      receiveAck()
     *      sendDataWithError()
     *      sendFileWithTimeout()
     *		sendFileWithGBN()
     * Do not change any method signatures and do not change any other methods or code provided.
     ***************************************************************************************************************************************
     **************************************************************************************************************************************/
    /*
     * This method sends protocol metadata to the server.
     * Sending metadata starts a transfer by sending the following information to the server in the metadata object (defined in MetaData.java):
     *      size - the size of the file to send
     *      name - the name of the file to create on the server
     *      maxSegSize - The size of the payload of the data segment
     * deal with error in sending
     * output relevant information messages for the user to follow progress of the file transfer.
     * This method does not set any of the attributes of the protocol.
     */
    public void sendMetadata() {
        try {
            // I have created a MetaData object and assigned its values.
            MetaData metaData = new MetaData();
            metaData.setSize(this.fileSize); // Set file size
            metaData.setName(this.outputFileName); // Set file name
            metaData.setMaxSegSize(this.maxPayload); // Set max payload size

            // In this case, I utilised an ObjectOutputStream to serialise MetaData to bytes.
            ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
            ObjectOutputStream objStream = new ObjectOutputStream(byteStream);
            objStream.writeObject(metaData);
            objStream.flush();
            byte[] metaDataBytes = byteStream.toByteArray();

            // Here I created a DatagramPacket to send the metadata
            DatagramPacket packet = new DatagramPacket(metaDataBytes, metaDataBytes.length, this.ipAddress, this.portNumber);

            // I've added this to send the metadata packet through the socket
            this.socket.send(packet);

            // An output message to confirm metadata sent
            System.out.println("SENDER: meta data is sent (file name, size, payload size): ("
                    + this.outputFileName + ", " + this.fileSize + ", " + this.maxPayload + ")");
            System.out.println("----------------------------------------------------");

        } catch (IOException e) {
            // Here I've used this to handle IO exceptions
            System.err.println("SENDER: Error while sending metadata: " + e.getMessage());
        }
    }



    public int readData() {
        FileInputStream fileInputStream = null; //I've declared the FileInputStream locally

        try {
            // Here it will initialize the FileInputStream
            fileInputStream = new FileInputStream(this.inputFile);

            // I made this to move the file pointer to the correct position by skipping already read bytes
            long bytesToSkip = this.fileSize - this.remainingBytes;
            fileInputStream.skip(bytesToSkip);

            // This will check if there are still bytes remaining to be read
            if (this.remainingBytes <= 0) {
                return -1;
            }

            // As for here it will create a buffer to hold the data chunk (up to maxPayload size)
            int bytesToRead = (int) Math.min(this.maxPayload, this.remainingBytes); // This will read remaining bytes if less than maxPayload
            byte[] buffer = new byte[bytesToRead];

            // It will Read data from the file into the buffer
            int bytesRead = fileInputStream.read(buffer, 0, bytesToRead);

            // If no more data to read, it will return -1
            if (bytesRead == -1) {
                return -1;
            }

            // I've added this to convert the buffer to a string for the payload
            String payload = new String(buffer, 0, bytesRead);

            // I have Set the data segment's payload
            this.dataSeg.setPayLoad(payload);
            this.dataSeg.setSize(bytesRead);  // Then I have set the size of the segment
            this.dataSeg.setType(SegmentType.Data);  // Finally I've set the segment type to "Data"

            // This will toggle the sequence number between 0 and 1
            int newSq = (this.dataSeg.getSq() == 0) ? 1 : 0;
            this.dataSeg.setSq(newSq);

            // This will update remaining and sent bytes
            this.remainingBytes -= bytesRead;
            this.sentBytes += bytesRead;

            return 0;

        } catch (IOException e) {
            System.err.println("SENDER: Error reading file: " + e.getMessage());
            System.exit(1);
        } finally {
            try {
                if (fileInputStream != null) {
                    fileInputStream.close();
                }
            } catch (IOException e) {
                System.err.println("SENDER: Error closing file input stream: " + e.getMessage());
            }
        }
        return -1;
    }





    /*
     * This method sends the current data segment (dataSeg) to the server
     * This method:
     * 		computes a checksum of the data and sets the data segment's checksum prior to sending.
     * output relevant information messages for the user to follow progress of the file transfer.
     */
    public void sendData() {
        try {
            // Add separator line before sending the segment

            // Calculate checksum for the payload
            int checksumValue = checksum(this.dataSeg.getPayLoad(), false);
            this.dataSeg.setChecksum(checksumValue);

            // Serialize the data segment into bytes
            ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
            ObjectOutputStream objStream = new ObjectOutputStream(byteStream);
            objStream.writeObject(this.dataSeg);
            objStream.flush();
            byte[] segmentBytes = byteStream.toByteArray();

            // Create a DatagramPacket to send the data segment
            DatagramPacket packet = new DatagramPacket(segmentBytes, segmentBytes.length, this.ipAddress, this.portNumber);

            // Send the packet
            this.socket.send(packet);

            // Increment total segments
            this.totalSegments++;

            // Print progress message
            System.out.println("SENDER: Sending segment: sq: " + this.dataSeg.getSq() + ", size: " + this.dataSeg.getSize() + ", checksum: " + checksumValue + ", content: (" + this.dataSeg.getPayLoad() + ")");

        } catch (IOException e) {
            System.err.println("SENDER: Error sending data segment: " + e.getMessage());
        }
    }




//Decide on the right place to :
    // *  	update the remaining bytes so that it records the remaining bytes to be read from the file after this segment is transferred. When all file bytes have been read, the remaining bytes will be zero
    // *    update the number of total sent segments
    // *    update the number of sent bytes


    /*
     * This method receives the current Ack segment (ackSeg) from the server
     * This method:
     * 		needs to check whether the ack is as expected
     * 		exit of the client on detection of an error in the received Ack
     * return true if no error
     * output relevant information messages for the user to follow progress of the file transfer.
     */
    public boolean receiveAck(int expectedDataSq) {
        try {
            // Create a buffer to receive the ACK segment
            byte[] incomingData = new byte[1024];
            DatagramPacket incomingPacket = new DatagramPacket(incomingData, incomingData.length);

            // Wait for the incoming ACK from the server (this will throw SocketTimeoutException if it times out)
            this.socket.receive(incomingPacket);

            // Deserialize the ACK segment
            ByteArrayInputStream byteStream = new ByteArrayInputStream(incomingPacket.getData());
            ObjectInputStream objStream = new ObjectInputStream(byteStream);
            this.ackSeg = (Segment) objStream.readObject();

            // Check if the ACK sequence number matches the expected one
            if (this.ackSeg.getSq() == expectedDataSq) {
                System.out.println("SENDER: ACK sq= " + this.ackSeg.getSq() + " RECEIVED.");
                System.out.println("----------------------------------------");
                return true;
            } else {
                System.err.println("SENDER: Received incorrect ACK. Expected: " + expectedDataSq + ", but got: " + this.ackSeg.getSq());
                return false;  // Indicating incorrect ACK
            }

        } catch (SocketTimeoutException e) {
            // Handle timeout
            System.err.println("SENDER: Timeout occurred while waiting for ACK.");
            return false;  // Indicating timeout, so caller can handle retries
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("SENDER: Error receiving ACK: " + e.getMessage());
            return false;
        }
    }





    /*
     * This method sends the current data segment (dataSeg) to the server with errors
     * This method:
     * 	 	may  corrupt the checksum according to the loss probability specified if the transfer mode is with timeout (wt)
     * 		If the count of consecutive retries/retransmissions exceeds the maximum number of allowed retries, the method exits the client with an
     * appropriate error message.
     *	This method does not receive any segment from the server
     * output relevant information messages for the user to follow progress of the file transfer.
     */
    public void sendDataWithError() throws IOException {
        try {
            // Check if the segment should be corrupted based on the loss probability
            boolean corrupted = isCorrupted(this.lossProb);

            // Calculate checksum for the payload, corrupt it if needed
            int checksumValue = checksum(this.dataSeg.getPayLoad(), corrupted);
            this.dataSeg.setChecksum(checksumValue);

            // Serialize the data segment into bytes
            ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
            ObjectOutputStream objStream = new ObjectOutputStream(byteStream);
            objStream.writeObject(this.dataSeg);
            objStream.flush();
            byte[] segmentBytes = byteStream.toByteArray();

            // Create a DatagramPacket to send the data segment
            DatagramPacket packet = new DatagramPacket(segmentBytes, segmentBytes.length, this.ipAddress, this.portNumber);

            // Send the packet
            this.socket.send(packet);

            // Increment the total segments count here
            this.totalSegments++;  // Make sure this is being updated

            // Print progress message with corruption info
            if (corrupted) {
                System.out.println("SENDER: Segment corrupted. Sending corrupted segment: sq: " + this.dataSeg.getSq() + ", size: " + this.dataSeg.getSize() + ", checksum: 0, content: (" + this.dataSeg.getPayLoad() + ")");
            } else {
                System.out.println("SENDER: Sending segment: sq: " + this.dataSeg.getSq() + ", size: " + this.dataSeg.getSize() + ", checksum: " + checksumValue + ", content: (" + this.dataSeg.getPayLoad() + ")");
            }

        } catch (IOException e) {
            System.err.println("SENDER: Error sending data segment with error: " + e.getMessage());
        }
    }




    /*
     * This method transfers the given file using the resources provided by the protocol structure.
     *
     * This method is similar to the sendFileNormal method except that it resends data segments if no ACK for a segment is received from the server.
     * This method:
     *  simulates network corruption of some data segments by injecting corruption into segment checksums (using sendDataWithError() method).
     *  will timeout waiting for an ACK for a corrupted segment and will resend the same data segment.
     *  updates attributes that record the progress of a file transfer. This includes the number of consecutive retries for each segment.
     *
     * output relevant information messages for the user to follow progress of the file transfer.
     * after completing the file transfer, display total segments transferred and the total number of resent segments
     *
     * relevant methods that need to be used include: readData(), sendDataWithError(), receiveAck().
     */
    void sendFileWithTimeout() throws IOException {
        // Set the socket timeout (in milliseconds)
        this.socket.setSoTimeout(this.timeout * 1000); // Timeout set to 10 seconds

        while (this.remainingBytes != 0) {
            // Read the next chunk of data from the file
            if (readData() == -1) {
                break; // End of file, no more data to send
            }

            // Send the data with possible corruption
            sendDataWithError();

            int retries = 0; // Keep track of the number of retries for this segment

            // Wait for ACK or retransmit if no ACK received
            while (retries < this.maxRetries) {
                boolean ackReceived = receiveAck(this.dataSeg.getSq());
                if (ackReceived) {
                    // ACK received, break out of retry loop
                    break;
                } else {
                    retries++; // Increment the retry count
                    if (retries >= this.maxRetries) {
                        System.err.println("SENDER: Max retries reached. Terminating client.");
                        System.exit(1); // Terminate after max retries
                    }
                    System.out.println("SENDER: TIMEOUT ALERT: Re-sending the same segment again, current retry: " + retries);
                    sendDataWithError(); // Re-send the segment (could be corrupted)
                    this.resentSegments++; // Track the number of resent segments
                }
            }
        }

        // After the file transfer is complete
        System.out.println("Total Segments Sent: " + this.totalSegments);
        System.out.println("Re-sent Segments: " + this.resentSegments);
    }




    /*
     *  transfer the given file using the resources provided by the protocol structure using GoBackN.
     */
    void sendFileNormalGBN(int window) throws IOException {
        int base = 0;
        int nextSeqNum = 0;
        int expectedAck = 0;
        boolean transferComplete = false;
        int totalSegments = 0;

        System.out.println("---------------Sending the segments in the initial window --------------------------");

        while (!transferComplete) {
            // Send segments within the window and if there are bytes left to read
            while (nextSeqNum < base + window && remainingBytes > 0) {
                if (readData() == -1) {
                    System.out.println("SENDER: End of file reached.");
                    break;
                }

                int seqNum = nextSeqNum % (window + 1);
                dataSeg.setSq(seqNum);
                sendData();
                totalSegments++;
                nextSeqNum++;
            }

            // Display the current outstanding ACKs
            System.out.println("\n-----------------------------------------------------------");
            System.out.println("SENDER: Waiting for an ack and slide the window if the ack number is correct");
            System.out.println("-----------------------------------------------------------");
            System.out.print("SENDER: current outstanding Acks [ ");
            for (int i = base; i < nextSeqNum; i++) {
                System.out.print(i % (window + 1) + " ");
            }
            System.out.println("]");

            // Receive acknowledgments and slide the window as needed
            while (expectedAck < nextSeqNum) {
                if (receiveAck(expectedAck % (window + 1))) {
                    base++;
                    expectedAck++;

                    if (remainingBytes > 0 && nextSeqNum < base + window) {
                        System.out.println("SENDER: slide the window and send the next segment");
                        if (readData() != -1) {
                            int seqNum = nextSeqNum % (window + 1);
                            dataSeg.setSq(seqNum);
                            sendData();
                            totalSegments++;
                            nextSeqNum++;

                            // Display updated outstanding ACKs
                            System.out.println("\n-----------------------------------------------------------");
                            System.out.print("SENDER: current outstanding Acks [ ");
                            for (int i = base; i < nextSeqNum; i++) {
                                System.out.print(i % (window + 1) + " ");
                            }
                            System.out.println("]");
                        }
                    }
                } else {
                    // Break if the ACK is not the expected one
                    break;
                }
            }

            // Complete transfer when all segments are sent and acknowledged
            if (remainingBytes <= 0 && expectedAck >= nextSeqNum) {
                transferComplete = true;
            }
        }

        // Final message with total segments sent
        System.out.println("Total segments sent: " + totalSegments);
    }
















    /*************************************************************************************************************************************
     **************************************************************************************************************************************
     **************************************************************************************************************************************
     These methods are implemented for you .. Do NOT Change them
     **************************************************************************************************************************************
     **************************************************************************************************************************************
     **************************************************************************************************************************************/
    /*
     * This method initialises ALL the 19 attributes needed to allow the Protocol methods to work properly
     */
    public void initProtocol(String hostName , String portNumber, String fileName, String outputFileName, String payloadSize, String mode) throws UnknownHostException, SocketException {
        this.portNumber = Integer.parseInt(portNumber);
        this.ipAddress = InetAddress.getByName(hostName);
        this.socket = new DatagramSocket();
        this.inputFile = checkFile(fileName);
        this.inputFileName = fileName;
        this.outputFileName =  outputFileName;
        this.fileSize       =this.inputFile.length();

        this.remainingBytes = this.fileSize;
        this.maxPayload = Integer.parseInt(payloadSize);
        this.mode = mode;
        this.dataSeg = new Segment();
        this.ackSeg = new Segment();

        this.timeout = DEFAULT_TIMEOUT;
        this.maxRetries = DEFAULT_RETRIES;

        this.sentBytes = 0;
        this.lossProb =0;
        this.totalSegments =0;
        this.resentSegments = 0;
        this.currRetry = 0;
    }

    /* transfer the given file using the resources provided by the protocol
     *      attributes, according to the normal file transfer without timeout
     *      or retransmission (for part 2).
     */
    public void sendFileNormal() throws IOException {
        while (this.remainingBytes!=0) {
            readData();
            sendData();
            if(!receiveAck(this.dataSeg.getSq()))  System.exit(0);
        }
        System.out.println("Total Segments "+ this.totalSegments );
    }

    /* calculate the segment checksum by adding the payload
     * Parameters:
     * payload - the payload string
     * corrupted - a boolean to indicate whether the checksum should be corrupted
     *      to simulate a network error
     *
     * Return:
     * An integer value calculated from the payload of a segment
     */
    public static int checksum(String payload, Boolean corrupted)
    {
        if (!corrupted)
        {
            int i;

            int sum = 0;
            for (i = 0; i < payload.length(); i++)
                sum += (int)payload.charAt(i);
            return sum;
        }
        return 0;
    }

    /* used by Client.java to set the loss probability (for part 3)*/
    public void setLossProb(float loss) {
        this.lossProb = loss;
    }

    /*
     * returns true with the given probability
     *
     * The result can be passed to the checksum function to "corrupt" a
     * checksum with the given probability to simulate network errors in
     * file transfer.
     *
     */
    private static Boolean isCorrupted(float prob) {

        double randomValue = Math.random();  //0.0 to 99.9
        return randomValue <= prob;
    }

    /* check if the input file does exist before sending it */
    private static File checkFile(String fileName)
    {
        File file = new File(fileName);
        if(!file.exists()) {
            System.out.println("SENDER: File does not exists");
            System.out.println("SENDER: Exit ..");
            System.exit(0);
        }
        return file;
    }
}