package edu.stanford.arcspread;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * This class wraps commands that arrive either via
 * HTTP (encoded in a URL), or via ROS (in a ROS
 * message. The API is a HashMap. Keys are command
 * arguments; values are the respective actual parameters.
 * The command name is available as well.
 * @author Paepcke
 *
 */
public class Command implements Map<String,String> {
	
	String commandName = null;
	HashMap<String,String> args = new HashMap<String,String>();
	
	public Command(String theCommandName, String[] attr_eq_val_strings) {
		this.commandName = theCommandName;
		for (String attrVal : attr_eq_val_strings) {
			String [] attrAndVal = attrVal.split("=");
			args.put(attrAndVal[0], attrAndVal[1]);
		}
	}

	public Command() {
		
	}
	
	public void setCommandName(String name) {
		commandName = name;
	}
	
	public String getCommandName() {
		return commandName;
	}
	
	public void clear() {
		args.clear();
	}

	public boolean containsKey(Object key) {
		return args.containsKey((String) key);
	}

	public boolean containsValue(Object value) {
		return args.containsValue((String) value);
	}

	public Set<java.util.Map.Entry<String, String>> entrySet() {
		return args.entrySet();
	}

	public String get(Object key) {
		return args.get(key);
	}

	public boolean isEmpty() {
		return args.isEmpty();
	}

	public Set<String> keySet() {
		return args.keySet();
	}

	public String remove(Object key) {
		return args.remove(key);
	}

	public int size() {
		return args.size();
	}

	public Collection<String> values() {
		return args.values();
	}

	public String put(String key, String value) {
		return args.put(key, value);
	}

	public void putAll(Map<? extends String, ? extends String> theMap) {
		args.putAll(theMap);
	}
	
	public String toString() {
		String res = "Command[" + getCommandName() + ": ";
		for (String key : args.keySet()) {
			res += key + "=" + args.get(key)  + " ";
		}
		res = res.trim();
		res += "]";
		return res;
	}
	
}
