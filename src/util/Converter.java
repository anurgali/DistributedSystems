package util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

public class Converter {

	// http://stackoverflow.com/questions/2836646/java-serializable-object-to-byte-array
	public static byte[] convertToBytes(Object output) throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutput out = null;
		try {
			out = new ObjectOutputStream(bos);
			out.writeObject(output);
			byte[] outputInBytes = bos.toByteArray();
			return outputInBytes;
		} finally {
			try {
				if (out != null) {
					out.close();
				}
			} catch (IOException ex) {
			}
			try {
				bos.close();
			} catch (IOException ex) {
			}
		}
	}
	
	public static Object createObject(byte[] objectInBytes) throws IOException, ClassNotFoundException {
		ByteArrayInputStream bis = new ByteArrayInputStream(objectInBytes);
		ObjectInput in = null;
		try {
			in = new ObjectInputStream(bis);
			Object o = in.readObject(); 		
			return o;
		} finally {
			try {
				bis.close();
			} catch (IOException ex) {}
			try {
				if (in != null) {
					in.close();
				}
			} catch (IOException ex) {}
		}
	}

}
