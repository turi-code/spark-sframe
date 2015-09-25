package net.razorvine.pickle.custom;

import java.io.IOException;
import java.io.OutputStream;

import net.razorvine.pickle.PickleException;

/**
 * This file is borrowed directly from pyspark-cassandra (thanks!)
 *
 * https://github.com/TargetHolding/pyspark-cassandra/
 *
 * It uses a customized razorvine Pickler with better implementation of
 * pickling of Scala Maps and Tuples (again thank you!) to convert an Iterator
 * over java objects to an Iterator over pickled python objects.
 *
 */

/**
 * Interface for Object Picklers used by the pickler, to pickle custom classes. 
 *
 * @author Irmen de Jong (irmen@razorvine.net)
 */
public interface IObjectPickler {
	/**
	 * Pickle an object.
	 */
	public void pickle(Object o, OutputStream out, Pickler currentPickler) throws PickleException, IOException;
}
