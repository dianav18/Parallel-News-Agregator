import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Collections;

public class Tema1 {
	public static List<String> inputFiles = new ArrayList<>();

	//variables for choosing unique articles
	public static ConcurrentHashMap<String, FieldOfInterest> uuidArticlesSelection = new ConcurrentHashMap<>();
	public static ConcurrentHashMap<String, FieldOfInterest> titleArticlesSelection = new ConcurrentHashMap<>();

	// variable for synchronization
	public static final Object deduplicationLock = new Object();

	public static CyclicBarrier barrier;

	public static int numberOfFiles;
	public static int numberOfAuxilaryFiles;

	// variables for file processing
	public static Set<String> categories = new HashSet<>();
	public static Set<String> languages = new HashSet<>();
	public static Set<String> excludedWords = new HashSet<>();

	public static ConcurrentHashMap<String, List<String>> articlesByCategory = new ConcurrentHashMap<>();
	public static ConcurrentHashMap<String, List<String>> articlesByLanguage = new ConcurrentHashMap<>();

	//what?
	public static ConcurrentHashMap<String, AtomicInteger> keywordCounts = new ConcurrentHashMap<>();
	public static AtomicInteger totalArticlesRead = new AtomicInteger(0);

	//useful for splitting the unique articles among the threads
	public static List<FieldOfInterest> uniqueArticlesList;

	public static Set<String> blacklistedUuids = new HashSet<>();
	public static Set<String> blacklistedTitles = new HashSet<>();

	public static void main(String[] args) throws IOException {
		if (args.length <3) {
//			System.err.println("Usage: java Tema1 <numOfThreads> <articlesFile> <auxilaryFile>");
			return;
		}

		int numOfThreads = Integer.parseInt(args[0]);
		String articlesFile = args[1];
		String auxilaryFile = args[2];

		fileReader(articlesFile);
		auxilaryFileReader(auxilaryFile);

		barrier = new CyclicBarrier(numOfThreads);

		Thread[] threads = new Thread[numOfThreads];

		for (int i = 0; i < numOfThreads; i++) {
			threads[i] = new MyThread(i, numOfThreads);
			threads[i].start();
		}

		for (int i = 0; i < numOfThreads; i++) {
			try {
				threads[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		List<FieldOfInterest> sortedArticlesByFieldPublished = new ArrayList<>(uuidArticlesSelection.values());

		sortedArticlesByFieldPublished.sort((a1, a2) -> {
			int dateComparison = a2.published.compareTo(a1.published);

			if (dateComparison == 0) {
				return a1.uuid.compareTo(a2.uuid);
			}
			return dateComparison;
		});

		try (BufferedWriter writer = new BufferedWriter(new FileWriter("all_articles.txt"))) {
			for (FieldOfInterest art : sortedArticlesByFieldPublished) {
				writer.write(art.uuid + " " + art.published);
				writer.newLine();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		sortBy(articlesByCategory);
		sortBy(articlesByLanguage);

		List<Map.Entry<String, AtomicInteger>> sortedKeywords = new ArrayList<>(keywordCounts.entrySet());

		sortedKeywords.sort((e1, e2) -> {
			int count1 = e1.getValue().get();
			int count2 = e2.getValue().get();
			if (count1 != count2) {
				return Integer.compare(count2, count1); // Descrescător
			}
			return e1.getKey().compareTo(e2.getKey()); // Lexicografic
		});

		try (BufferedWriter writer = new BufferedWriter(new FileWriter("keywords_count.txt"))) {
			for (Map.Entry<String, AtomicInteger> entry : sortedKeywords) {
				writer.write(entry.getKey() + " " + entry.getValue().get());
				writer.newLine();
			}
		} catch (IOException e) { e.printStackTrace(); }

		int totalRead = totalArticlesRead.get();
		int finalCount = uuidArticlesSelection.size();
		int duplicatesFound = totalRead - finalCount;

		Map<String, Integer> authorCounts = new HashMap<>();
		for (FieldOfInterest art : uuidArticlesSelection.values()) {
			authorCounts.put(art.author, authorCounts.getOrDefault(art.author, 0) + 1);
		}
		String bestAuthor = "";
		int maxAuthorCount = -1;
		for (Map.Entry<String, Integer> entry : authorCounts.entrySet()) {
			if (entry.getValue() > maxAuthorCount) {
				maxAuthorCount = entry.getValue();
				bestAuthor = entry.getKey();
			} else if (entry.getValue() == maxAuthorCount) {
				if (bestAuthor.compareTo(entry.getKey()) > 0) bestAuthor = entry.getKey();
			}
		}

		String topLanguage = "";
		int maxLangCount = -1;
		for (Map.Entry<String, List<String>> entry : articlesByLanguage.entrySet()) {
			int count = entry.getValue().size();
			if (count > maxLangCount) {
				maxLangCount = count;
				topLanguage = entry.getKey();
			} else if (count == maxLangCount) {
				if (topLanguage.compareTo(entry.getKey()) > 0) topLanguage = entry.getKey();
			}
		}

		String topCategory = "";
		int maxCatCount = -1;
		for (Map.Entry<String, List<String>> entry : articlesByCategory.entrySet()) {
			int count = entry.getValue().size();
			if (count > maxCatCount) {
				maxCatCount = count;
				topCategory = entry.getKey();
			} else if (count == maxCatCount) {
				if (topCategory.compareTo(entry.getKey()) > 0) topCategory = entry.getKey();
			}
		}
		String topCategoryNorm = topCategory.replace(",", "").replace(" ", "_");

		FieldOfInterest mostRecent = sortedArticlesByFieldPublished.isEmpty() ? null : sortedArticlesByFieldPublished.get(0);

		String topKeyword = "";
		int maxKeyCount = 0;
		if (!sortedKeywords.isEmpty()) {
			topKeyword = sortedKeywords.get(0).getKey();
			maxKeyCount = sortedKeywords.get(0).getValue().get();
		}

		try (BufferedWriter writer = new BufferedWriter(new FileWriter("reports.txt"))) {
			writer.write("duplicates_found - ");
			writer.write(String.valueOf(duplicatesFound)); writer.newLine();

			writer.write("unique_articles - ");
			writer.write(String.valueOf(finalCount)); writer.newLine();

			writer.write("best_author - " + bestAuthor + " " + maxAuthorCount); writer.newLine();

			if (!topLanguage.isEmpty()) {
				writer.write("top_language - " + topLanguage + " " + maxLangCount); writer.newLine();
			}

			if (!topCategoryNorm.isEmpty()) {
				writer.write("top_category - " + topCategoryNorm + " " + maxCatCount); writer.newLine();
			}

			if (mostRecent != null) {
				writer.write("most_recent_article - ");
				writer.write(mostRecent.published + " " + mostRecent.url); writer.newLine();
			}

			if (!topKeyword.isEmpty()) {
				writer.write("top_keyword_en - " + topKeyword + " " + maxKeyCount); writer.newLine();
			}
		} catch (IOException e) { e.printStackTrace(); }
	}

	private static void sortBy(ConcurrentHashMap<String, List<String>> articlesBy) {
		articlesBy.forEach((languageName, uuids) -> {
			Collections.sort(uuids);
			String fileName = languageName.replace(",", "").replace(" ", "_") + ".txt";
			try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
				for (String id : uuids) {
					writer.write(id);
					writer.newLine();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
	}

	private static void auxilaryFileReader(String auxilaryFile) throws IOException {
		File file = new File(auxilaryFile);
		String parentDir = file.getParent();

		BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
		bufferedReader.readLine();

		String languageRel = bufferedReader.readLine().trim();
		String categoriesRel = bufferedReader.readLine().trim();
		String wordsRel = bufferedReader.readLine().trim();
		bufferedReader.close();

		loadSet(new File(parentDir, languageRel).getCanonicalPath(), languages);
		loadSet(new File(parentDir, categoriesRel).getCanonicalPath(), categories);
		loadSet(new File(parentDir, wordsRel).getCanonicalPath(), excludedWords);
	}

	private static void fileReader(String articlesFile) throws IOException {
		File file = new File(articlesFile);
		String parentDir = file.getParent();

		BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
		String line = bufferedReader.readLine();
		numberOfFiles = Integer.parseInt(line);

		while ((line = bufferedReader.readLine()) != null) {
			if (!line.trim().isEmpty()) {
				String relativePath = line.trim();
				File fullPath = new File(parentDir, relativePath);
				inputFiles.add(fullPath.getCanonicalPath());
			}
		}
		bufferedReader.close();
	}

	private static void loadSet(String path, Set<String> targetSet) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(path));
		String line = br.readLine();
		while ((line = br.readLine()) != null) {
			if (!line.trim().isEmpty()) {
				targetSet.add(line.trim());
			}
		}
		br.close();
	}
}