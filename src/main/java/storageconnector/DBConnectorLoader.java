/**
 * 
 */
package storageconnector;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import exceptions.FBaseStorageConnectorException;

/**
 * Loads implementations of {@link AbstractDBConnector} from external jar
 * files.
 * 
 * @author Dave (adapted from https://stackoverflow.com/a/20586806)
 * 
 * 
 *
 */
public class DBConnectorLoader {

	/**
	 * adds the specified list of jar files to the classpath, loads
	 * implementations of {@link AbstractDBConnector} and instantiates one of those
	 * instances.
	 * 
	 * @param jars
	 * @return an instance of {@link AbstractDBConnector} as found in the specified jar
	 *         files or null.
	 * @throws FBaseStorageConnectorException
	 */
	public AbstractDBConnector createConnectorFromJars(String... jars)
			throws FBaseStorageConnectorException {
		List<URL> jarFileURLs = new ArrayList<>();
		List<File> jarFiles = new ArrayList<>();
		try {
			for (String jar : jars) {
				// System.out.println("Processing jar: "+ jar);
				File jarFile = new File(jar);
				if (jarFile == null || !jarFile.exists())
					throw new IllegalArgumentException(
							"Jar file does not exist or parameter value was null");
				jarFileURLs.add(jarFile.toURI().toURL());
				jarFiles.add(jarFile);
			}
			// create a class loader for the jars
			URL[] downloadURLs = jarFileURLs
					.toArray(new URL[jarFileURLs.size()]);
			URLClassLoader loader = URLClassLoader.newInstance(downloadURLs,
					getClass().getClassLoader());

			List<Class<?>> implementingClasses = findImplementingClassesInJarFiles(
					jarFiles, AbstractDBConnector.class, loader);
			for (Class<?> clazz : implementingClasses) {
				// assume there is a public default constructor available
				AbstractDBConnector instance = (AbstractDBConnector) clazz.newInstance();
				return instance;
			}
		} catch (Exception e) {
			throw new FBaseStorageConnectorException(
					"Could not load AbstractDBConnector.", e);

		}
		return null;

	}

	/**
	 * Scans a jar file for .class-files and returns a {@link Set} containing
	 * the full name of found classes (in the following form:
	 * packageName.className)
	 *
	 * @param file
	 *            jar file which should be searched for .class-files
	 * @return the full names of all class files found
	 * @throws IOException
	 *             If during processing of the jar file an error occurred
	 * @throws IllegalArgumentException
	 *             If either the provided file is null, does not exist or is no
	 *             jar file
	 */
	public Set<String> extractClassnamesFromJarFile(File file)
			throws IOException, IllegalArgumentException {
		// System.out.println("extracting classnames from file: " + file);
		if (file == null || !file.exists())
			throw new IllegalArgumentException(
					"File does not exist or parameter was null.");
		if (file.getName().endsWith(".jar")) {
			Set<String> foundClasses = new HashSet<>();
			try (JarFile jarFile = new JarFile(file)) {
				Enumeration<JarEntry> entries = jarFile.entries();
				while (entries.hasMoreElements()) {
					JarEntry entry = entries.nextElement();
					if (entry.getName().endsWith(".class")) {
						String name = entry.getName();
						name = name.substring(0, name.lastIndexOf(".class"));
						if (name.indexOf("/") != -1)
							name = name.replaceAll("/", ".");
						if (name.indexOf("\\") != -1)
							name = name.replaceAll("\\", ".");
						foundClasses.add(name);
						// System.out.println("found class: " + name);
					}
				}
			}
			return foundClasses;
		}
		throw new IllegalArgumentException("Provided file was not a jar file.");
	}

	/**
	 * Looks inside a jar file and searches for implementing classes of the
	 * specified interface.
	 * 
	 * @param files
	 *            the list of jar files which shall be scanned for
	 *            implementations of iface
	 * @param iface
	 *            The interface classes have to implement
	 * @param loader
	 *            The class loader the implementing classes got loaded with
	 * @return A {@link List} of implementing classes for the provided interface
	 *         inside the specified jar files
	 * @throws Exception
	 *             if an error occurred while processing
	 */
	public List<Class<?>> findImplementingClassesInJarFiles(List<File> files,
			Class<?> iface, ClassLoader loader) throws Exception {
		List<Class<?>> implementingClasses = new ArrayList<Class<?>>();
		Set<String> classFiles = new HashSet<>();
		// System.out.println("Extracting classnames from files: " + files);
		for (File f : files)
			classFiles.addAll(extractClassnamesFromJarFile(f));
		// System.out.println("Checking whether classes implement interface "
		// + iface.getName());
		// scan the jar file for all included classes
		for (String classFile : classFiles) {
			// System.out.println("checking class: " + classFile);
			Class<?> clazz;
			// now try to load the class
			if (loader == null)
				clazz = Class.forName(classFile);
			else
				clazz = Class.forName(classFile, true, loader);

			// check if the class implements the provided interface
			if (iface.isAssignableFrom(clazz) && !clazz.equals(iface)) {
				implementingClasses.add(clazz);
				// System.out.println(clazz.getName() + " implements "
				// + iface.getName());
			}

		}
		return implementingClasses;
	}

}
