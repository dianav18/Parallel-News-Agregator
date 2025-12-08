import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.atomic.AtomicInteger;

public class MyThread extends Thread {
	private int id;
	private int numOfThreads;
	private ObjectMapper objectMapper;

	public MyThread(final int id, final int numOfThreads) {
		this.id = id;
		this.numOfThreads = numOfThreads;
		this.objectMapper = new ObjectMapper();
	}

	public void run() {
		final int numberOfFiles = Tema1.numberOfFiles;

        // First phase: Process files and select unique articles
		final int start = id * numberOfFiles / numOfThreads;
		final int end = (id + 1) * numberOfFiles / numOfThreads;

        // Each thread processes its assigned files
		for (int i = start; i < end; i++) {
			final String fileName = Tema1.inputFiles.get(i);
			processFile(fileName);
		}

		try {
            // first barrier: wait for all threads to finish file processing and deduplication from fhase one
			Tema1.barrier.await();

            // Only one thread creates the list of unique articles for the second phase
			if (id == 0) {
				Tema1.uniqueArticlesList = new ArrayList<>(Tema1.uuidArticlesSelection.values());
			}

            // second barrier: wait for the unique articles list to be ready
			Tema1.barrier.await();

            // Second phase: Process unique articles for categories, languages, and keywords
			final int totalUnique = Tema1.uniqueArticlesList.size();
			final int start2 = id * totalUnique / numOfThreads;
			final int end2 = (id + 1) * totalUnique / numOfThreads;

			for (int i = start2; i < end2; i++) {
				processUniqueArticle(Tema1.uniqueArticlesList.get(i));
			}

            // third barrier: wait for all threads to finish processing the second phase
			Tema1.barrier.await();
		} catch (final InterruptedException | BrokenBarrierException e) {
			throw new RuntimeException(e);
		}
	}

    // Method to process a single file and perform deduplication for the first phase
	private void processFile(final String fileName) {
		try {
			final File file = new File(fileName);

			final List<FieldOfInterest> articles = objectMapper.readValue(file, new TypeReference<List<FieldOfInterest>>(){});

			for (final FieldOfInterest article : articles) {
				if (article.uuid == null || article.title == null) {
					continue;
				}

				Tema1.totalArticlesRead.incrementAndGet();

				synchronized (Tema1.deduplicationLock) {
					if (Tema1.blacklistedUuids.contains(article.uuid) ||
							Tema1.blacklistedTitles.contains(article.title)) {
						continue;
					}

					final FieldOfInterest existingByUuid = Tema1.uuidArticlesSelection.get(article.uuid);
					final FieldOfInterest existingByTitle = Tema1.titleArticlesSelection.get(article.title);

					if (existingByUuid != null || existingByTitle != null) {
						Tema1.blacklistedUuids.add(article.uuid);
						Tema1.blacklistedTitles.add(article.title);

						if (existingByUuid != null) {
							Tema1.uuidArticlesSelection.remove(existingByUuid.uuid);
							Tema1.titleArticlesSelection.remove(existingByUuid.title);

							Tema1.blacklistedUuids.add(existingByUuid.uuid);
							Tema1.blacklistedTitles.add(existingByUuid.title);
						}
						if (existingByTitle != null) {
							Tema1.uuidArticlesSelection.remove(existingByTitle.uuid);
							Tema1.titleArticlesSelection.remove(existingByTitle.title);

							Tema1.blacklistedUuids.add(existingByTitle.uuid);
							Tema1.blacklistedTitles.add(existingByTitle.title);
						}
					} else {
						Tema1.uuidArticlesSelection.put(article.uuid, article);
						Tema1.titleArticlesSelection.put(article.title, article);
					}
				}
			}

		} catch (final IOException e) {
		}
	}

    // Method to process a unique article on categories for the second phase
	private void processUniqueArticle(final FieldOfInterest article) {
		if (article.categories != null) {
			final Set<String> distinctCategories = new HashSet<>(article.categories);

			for (final String category : distinctCategories) {
				if (Tema1.categories.contains(category)) {
					Tema1.articlesByCategory
							.computeIfAbsent(category, k -> Collections.synchronizedList(new ArrayList<>()))
							.add(article.uuid);
				}
			}
		}

		if (article.language != null && Tema1.languages.contains(article.language)) {
			Tema1.articlesByLanguage
					.computeIfAbsent(article.language, k -> Collections.synchronizedList(new ArrayList<>()))
					.add(article.uuid);
		}

		if ("english".equals(article.language) && article.text != null) {
			processKeywords(article.text);
		}
	}

	private void processKeywords(final String text) {
		final String content = text.toLowerCase();
		final String[] words = content.split("[\\s]+");

		final Set<String> uniqueWordsInArticle = new HashSet<>();

		for (final String rawWord : words) {
			final String cleanWord = rawWord.replaceAll("[^a-z]", "");

			if (!cleanWord.isEmpty() && !Tema1.excludedWords.contains(cleanWord)) {
				uniqueWordsInArticle.add(cleanWord);
			}
		}

		for (final String word : uniqueWordsInArticle) {
			Tema1.keywordCounts
					.computeIfAbsent(word, k -> new AtomicInteger(0))
					.incrementAndGet();
		}
	}
}