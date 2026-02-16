package pdc;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class Message {
    public String magic = "CSM218";
    public int version = 1;
    public String messageType = "NONE";
    public String sender = "Muzi";
    public String studentId = "N02221251V"; 
    public long timestamp = System.currentTimeMillis();
    public byte[] payload = new byte[0];

    public Message() {
        this.timestamp = System.currentTimeMillis();
    }

    public Message(String messageType, String sender, byte[] payload) {
        this.messageType = messageType;
        this.sender = sender;
        this.payload = (payload != null) ? payload : new byte[0];
        this.timestamp = System.currentTimeMillis();
    }

    /**
     * Converts the message to a byte stream.
     * Order: Magic -> Version -> MessageType -> Sender -> StudentId -> Timestamp -> Payload
     */
    public byte[] pack() {
        byte[] magicBytes = magic.getBytes(StandardCharsets.UTF_8);
        byte[] typeBytes = messageType.getBytes(StandardCharsets.UTF_8);
        byte[] senderBytes = sender.getBytes(StandardCharsets.UTF_8);
        byte[] idBytes = studentId.getBytes(StandardCharsets.UTF_8);

        // Calculate size including the new studentId field
        int size = 4 + magicBytes.length + 
                   4 + 
                   4 + typeBytes.length + 
                   4 + senderBytes.length + 
                   4 + idBytes.length + 
                   8 + 
                   4 + payload.length;

        ByteBuffer buffer = ByteBuffer.allocate(size);
        
        buffer.putInt(magicBytes.length);
        buffer.put(magicBytes);
        buffer.putInt(version);
        buffer.putInt(typeBytes.length);
        buffer.put(typeBytes);
        buffer.putInt(senderBytes.length);
        buffer.put(senderBytes);
        buffer.putInt(idBytes.length);
        buffer.put(idBytes);
        buffer.putLong(timestamp);
        buffer.putInt(payload.length);
        buffer.put(payload);

        return buffer.array();
    }

    public static Message unpack(byte[] data) {
        if (data == null || data.length < 12) {
            return null;
        }
        
        try {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            Message msg = new Message();

            int magicLen = buffer.getInt();
            if (magicLen < 0 || magicLen > 100) return null;
            byte[] magicB = new byte[magicLen];
            buffer.get(magicB);
            msg.magic = new String(magicB, StandardCharsets.UTF_8);
            
            // Validate magic string
            if (!"CSM218".equals(msg.magic)) {
                return null;
            }

            msg.version = buffer.getInt();

            int typeLen = buffer.getInt();
            if (typeLen < 0 || typeLen > 100) return null;
            byte[] typeB = new byte[typeLen];
            buffer.get(typeB);
            msg.messageType = new String(typeB, StandardCharsets.UTF_8);

            int senderLen = buffer.getInt();
            if (senderLen < 0 || senderLen > 100) return null;
            byte[] senderB = new byte[senderLen];
            buffer.get(senderB);
            msg.sender = new String(senderB, StandardCharsets.UTF_8);

            int idLen = buffer.getInt();
            if (idLen < 0 || idLen > 100) return null;
            byte[] idB = new byte[idLen];
            buffer.get(idB);
            msg.studentId = new String(idB, StandardCharsets.UTF_8);

            msg.timestamp = buffer.getLong();

            int payloadLen = buffer.getInt();
            if (payloadLen < 0 || payloadLen > buffer.remaining()) return null;
            byte[] payloadB = new byte[payloadLen];
            buffer.get(payloadB);
            msg.payload = payloadB;

            return msg;
        } catch (Exception e) {
            return null;
        }
    }
}
