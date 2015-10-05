import java.io.*;
import java.util.*;
import java.util.concurrent.Semaphore;

public class JavaWordCount {
	private static int Total_Threads = 1;
	private static int Total_Chunks = 16;
	private static List<String> FilesChunksList = new ArrayList<String>();
	private static HashMap<String, Integer> hashmap = new HashMap<String, Integer>();
	private static String fname = null;

	@SuppressWarnings({ "unchecked" })
	public static void main(String[] args) throws InterruptedException,	IOException {
		BufferedReader keyboard = new BufferedReader(new InputStreamReader(System.in));
		System.out.println("Enter the filename to be executed  : ");
		fname = keyboard.readLine().trim();
		System.out.println("Do you want to split files(y/n): ");
		String ch = keyboard.readLine().trim();
		if (ch.equalsIgnoreCase("y")) {
			splitTOChunks(fname);
			loadFilesChunkslist();
		} else {
			FilesChunksList.add(fname);
		}
		System.out.println("Enter no.of Threads: ");
		try {
			Total_Threads = Integer.parseInt(keyboard.readLine().trim());
		} catch (Exception e) {
		}
		Thread[] td = new Thread[Total_Threads];
		final Semaphore sem_files = new Semaphore(Total_Threads, false);
		long start = System.currentTimeMillis();
		for (int i = 0; i < Total_Threads; i++) {
			Thread.sleep(i * 10);
			td[i] = new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						sem_files.acquire();
						while (!FilesChunksList.isEmpty()) {
							String filename = FilesChunksList.remove(0);
							countWord(filename);
						}
						sem_files.release();
					} catch (Exception ex) {
					}
				}
			});
			td[i].start();
		}
		for (int i = 0; i < Total_Threads; i++) {
			td[i].join();
		}
		long end = System.currentTimeMillis();
		long diff = end - start;
		System.out.println("\nTotal time taken	: " + diff + " milliseconds");
		FileWriter fstream = new FileWriter("wordcount-java.txt");
		System.out.println("Total unique words	: " + hashmap.size());
		System.out.println("Sorting words into descending order of its frequency..");
		BufferedWriter out = new BufferedWriter(fstream);

		Object[] a = hashmap.entrySet().toArray();
		Arrays.sort(a, new Comparator<Object>() {
			public int compare(Object o1, Object o2) {
				return ((Map.Entry<String, Integer>) o2).getValue().compareTo(
						((Map.Entry<String, Integer>) o1).getValue());
			}
		});
		for (Object e : a) {
			String key = (String) ((Map.Entry<String, Integer>) e).getKey();
			int value = (Integer) ((Map.Entry<String, Integer>) e).getValue();
			out.write(key + ": ");
			out.write(Integer.toString(value));
			out.newLine();
		}

		out.close();
		System.out.println("Results are saved to file : wordcount-java.txt");
	}

	public static void loadFilesChunkslist() {
		String pFName = "input_0";
		for (int i = 1; i <= Total_Chunks; i++) {
			if (i >= 10)
				pFName = "input_";
			FilesChunksList.add(pFName + i + ".txt");
		}
	}

	public static void createFilesChunk(int start, long end, String chunkname) {
		File file = new File(fname);
		try {
			FileReader freader = new FileReader(file);
			LineNumberReader lnreader = new LineNumberReader(freader);
			String line = "";
			FileWriter fileWriter = new FileWriter(chunkname);
			BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
			while (lnreader.getLineNumber() != start) {
				line = lnreader.readLine();
			}
			lnreader.setLineNumber(start);
			while (lnreader.getLineNumber() != end) {
				line = lnreader.readLine();
				bufferedWriter.write(line);
				bufferedWriter.newLine();
			}
			bufferedWriter.close();
			lnreader.close();
			System.out.println(chunkname + " -> Line Starts:" + start
					+ " ends:" + end);
		} catch (Exception ex) {
		}
	}

	private static Semaphore sem = new Semaphore(Total_Threads, false);

	public static void countWord(String filename) {
		String s;
		BufferedReader br;
		try {
			System.out.println(Thread.currentThread().getName() + " -> "
					+ filename);
			br = new BufferedReader(new FileReader(filename));
			while ((s = br.readLine()) != null) {
				s = s.replaceAll("[^\\w\\s]", "");
				StringTokenizer stringTok = new StringTokenizer(s);
				int count = stringTok.countTokens();
				for (int l = 0; l < count; l++) {
					String iword = stringTok.nextToken();
					sem.acquire();
					hashmap.put(iword, hashmap.get(iword) == null ? 1
							: ((Integer) hashmap.get(iword) + 1));
					sem.release();
				}
			}
		} catch (Exception ex) {
		}
	}

	private static LineNumberReader lnreader;

	public static int getFileTotalLines(String fileName) {
		int tot_lines = 0;
		try {
			File file = new File(fileName);
			FileReader freader = new FileReader(file);
			lnreader = new LineNumberReader(freader);
			while ((lnreader.readLine()) != null) {
			}
			tot_lines = lnreader.getLineNumber();
		} catch (Exception ex) {
		}
		return tot_lines;
	}

	public static void splitTOChunks(String fileName) {
		int start = 0;
		System.out.println("Dividing file in to " + Total_Chunks
				+ " Chunks ...\n");
		int tot_lines = getFileTotalLines(fileName);
		int diff = tot_lines / Total_Chunks;
		int end = diff;
		String a;
		for (int i = 1; i <= Total_Chunks; i++) {
			a = (i < 10 ? "input_0" : "input_") + i + ".txt";
			createFilesChunk(start, end, a);
			start = end;
			end = end + diff;
			if ((i == (Total_Chunks - 1)) && (end != tot_lines))
				end = tot_lines;
		}
	}
}