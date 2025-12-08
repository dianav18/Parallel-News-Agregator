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

	// variables for file processing
	public static Set<String> categories = new HashSet<>();
	public static Set<String> languages = new HashSet<>();
	public static Set<String> excludedWords = new HashSet<>();

	public static ConcurrentHashMap<String, List<String>> articlesByCategory = new ConcurrentHashMap<>();
	public static ConcurrentHashMap<String, List<String>> articlesByLanguage = new ConcurrentHashMap<>();

	public static ConcurrentHashMap<String, AtomicInteger> keywordCounts = new ConcurrentHashMap<>();
	public static AtomicInteger totalArticlesRead = new AtomicInteger(0);

	//useful for splitting the unique articles among the threads
	public static List<FieldOfInterest> uniqueArticlesList;

	public static Set<String> blacklistedUuids = new HashSet<>();
	public static Set<String> blacklistedTitles = new HashSet<>();

	public static void main(final String[] args) throws IOException {
        if (args.length <3) {
			return;
		}

		final int numOfThreads = Integer.parseInt(args[0]);
		final String articlesFile = args[1];
		final String auxilaryFile = args[2];

        // read input files and auxiliary data
		fileReader(articlesFile);
		auxilaryFileReader(auxilaryFile);

        // initialize the barrier for thread synchronization
		barrier = new CyclicBarrier(numOfThreads);

        // start threads for processing the first and second phases
		final Thread[] threads = new Thread[numOfThreads];

		for (int i = 0; i < numOfThreads; i++) {
			threads[i] = new MyThread(i, numOfThreads);
			threads[i].start();
		}

        // wait for all threads to finish
		for (int i = 0; i < numOfThreads; i++) {
			try {
				threads[i].join();
			} catch (final InterruptedException e) {
				e.printStackTrace();
			}
		}

        // post processing and generating output files
		final List<FieldOfInterest> sortedArticlesByFieldPublished = new ArrayList<>(uuidArticlesSelection.values());

		sortedArticlesByFieldPublished.sort((a1, a2) -> {
			final int dateComparison = a2.published.compareTo(a1.published);

			if (dateComparison == 0) {
				return a1.uuid.compareTo(a2.uuid);
			}
			return dateComparison;
		});

		try (final BufferedWriter writer = new BufferedWriter(new FileWriter("all_articles.txt"))) {
			for (final FieldOfInterest art : sortedArticlesByFieldPublished) {
				writer.write(art.uuid + " " + art.published);
				writer.newLine();
			}
		} catch (final IOException e) {
			e.printStackTrace();
		}

		sortBy(articlesByCategory);
		sortBy(articlesByLanguage);

		final List<Map.Entry<String, AtomicInteger>> sortedKeywords = new ArrayList<>(keywordCounts.entrySet());

		sortedKeywords.sort((e1, e2) -> {
			final int count1 = e1.getValue().get();
			final int count2 = e2.getValue().get();
			if (count1 != count2) {
				return Integer.compare(count2, count1);
			}
			return e1.getKey().compareTo(e2.getKey());
		});

		try (final BufferedWriter writer = new BufferedWriter(new FileWriter("keywords_count.txt"))) {
			for (final Map.Entry<String, AtomicInteger> entry : sortedKeywords) {
				writer.write(entry.getKey() + " " + entry.getValue().get());
				writer.newLine();
			}
		} catch (final IOException e) {
            e.printStackTrace();
        }

		final int totalRead = totalArticlesRead.get();
		final int finalCount = uuidArticlesSelection.size();
		final int duplicatesFound = totalRead - finalCount;

		final Map<String, Integer> authorCounts = new HashMap<>();
		for (final FieldOfInterest art : uuidArticlesSelection.values()) {
			authorCounts.put(art.author, authorCounts.getOrDefault(art.author, 0) + 1);
		}
		String bestAuthor = "";
		int maxAuthorCount = -1;
		for (final Map.Entry<String, Integer> entry : authorCounts.entrySet()) {
			if (entry.getValue() > maxAuthorCount) {
				maxAuthorCount = entry.getValue();
				bestAuthor = entry.getKey();
			} else if (entry.getValue() == maxAuthorCount) {
				if (bestAuthor.compareTo(entry.getKey()) > 0) bestAuthor = entry.getKey();
			}
		}

		String topLanguage = "";
		int maxLangCount = -1;
		for (final Map.Entry<String, List<String>> entry : articlesByLanguage.entrySet()) {
			final int count = entry.getValue().size();
			if (count > maxLangCount) {
				maxLangCount = count;
				topLanguage = entry.getKey();
			} else if (count == maxLangCount) {
				if (topLanguage.compareTo(entry.getKey()) > 0) topLanguage = entry.getKey();
			}
		}

		String topCategory = "";
		int maxCatCount = -1;
		for (final Map.Entry<String, List<String>> entry : articlesByCategory.entrySet()) {
			final int count = entry.getValue().size();
			if (count > maxCatCount) {
				maxCatCount = count;
				topCategory = entry.getKey();
			} else if (count == maxCatCount) {
				if (topCategory.compareTo(entry.getKey()) > 0) topCategory = entry.getKey();
			}
		}
		final String topCategoryNorm = topCategory.replace(",", "").replace(" ", "_");

		final FieldOfInterest mostRecent = sortedArticlesByFieldPublished.isEmpty() ? null : sortedArticlesByFieldPublished.get(0);

		String topKeyword = "";
		int maxKeyCount = 0;
		if (!sortedKeywords.isEmpty()) {
			topKeyword = sortedKeywords.get(0).getKey();
			maxKeyCount = sortedKeywords.get(0).getValue().get();
		}

		try (final BufferedWriter writer = new BufferedWriter(new FileWriter("reports.txt"))) {
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
		} catch (final IOException e) {
            e.printStackTrace();
        }
	}

	private static void sortBy(final ConcurrentHashMap<String, List<String>> articlesBy) {
		articlesBy.forEach((languageName, uuids) -> {
			Collections.sort(uuids);
			final String fileName = languageName.replace(",", "").replace(" ", "_") + ".txt";
			try (final BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
				for (final String id : uuids) {
					writer.write(id);
					writer.newLine();
				}
			} catch (final IOException e) {
				e.printStackTrace();
			}
		});
	}

	private static void auxilaryFileReader(final String auxilaryFile) throws IOException {
		final File file = new File(auxilaryFile);
		final String parentDirectory = file.getParent();

		final BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
		bufferedReader.readLine();

		final String languageRel = bufferedReader.readLine().trim();
		final String categoriesRel = bufferedReader.readLine().trim();
		final String wordsRel = bufferedReader.readLine().trim();
		bufferedReader.close();

		loadSet(new File(parentDirectory, languageRel).getCanonicalPath(), languages);
		loadSet(new File(parentDirectory, categoriesRel).getCanonicalPath(), categories);
		loadSet(new File(parentDirectory, wordsRel).getCanonicalPath(), excludedWords);
	}

	private static void fileReader(final String articlesFile) throws IOException {
		final File file = new File(articlesFile);
		final String parentDirectory = file.getParent();

		final BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
		String line = bufferedReader.readLine();
		numberOfFiles = Integer.parseInt(line);

		while ((line = bufferedReader.readLine()) != null) {
			if (!line.trim().isEmpty()) {
				final String relativePath = line.trim();
				final File fullPath = new File(parentDirectory, relativePath);
				inputFiles.add(fullPath.getCanonicalPath());
			}
		}
		bufferedReader.close();
	}

	private static void loadSet(final String path, final Set<String> targetSet) throws IOException {
		final BufferedReader bufferedReader = new BufferedReader(new FileReader(path));
		String line = bufferedReader.readLine();
		while ((line = bufferedReader.readLine()) != null) {
			if (!line.trim().isEmpty()) {
				targetSet.add(line.trim());
			}
		}
        bufferedReader.close();
	}
}