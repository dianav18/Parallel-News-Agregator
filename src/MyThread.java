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

	public MyThread(int id, int numOfThreads) {
		this.id = id;
		this.numOfThreads = numOfThreads;
		this.objectMapper = new ObjectMapper();
	}

	public void run() {
		int numberOfFiles = Tema1.numberOfFiles;

		int start = id * numberOfFiles / numOfThreads;
		int end = (id + 1) * numberOfFiles / numOfThreads;

		for (int i = start; i < end; i++) {
			String fileName = Tema1.inputFiles.get(i);
			processFile(fileName);
		}

		try {
			Tema1.barrier.await();

			if (id == 0) {
				Tema1.uniqueArticlesList = new ArrayList<>(Tema1.uuidArticlesSelection.values());
			}

			Tema1.barrier.await();

			int totalUnique = Tema1.uniqueArticlesList.size();
			int start2 = id * totalUnique / numOfThreads;
			int end2 = (id + 1) * totalUnique / numOfThreads;

			for (int i = start2; i < end2; i++) {
				processUniqueArticle(Tema1.uniqueArticlesList.get(i));
			}

			Tema1.barrier.await();
		} catch (InterruptedException | BrokenBarrierException e) {
			throw new RuntimeException(e);
		}
	}

	private void processFile(String fileName) {
		try {
			File file = new File(fileName);

			List<FieldOfInterest> articles = objectMapper.readValue(file, new TypeReference<List<FieldOfInterest>>(){});

			for (FieldOfInterest article : articles) {
				if (article.uuid == null || article.title == null) {
					continue;
				}

				Tema1.totalArticlesRead.incrementAndGet();

				synchronized (Tema1.deduplicationLock) {
					if (Tema1.blacklistedUuids.contains(article.uuid) ||
							Tema1.blacklistedTitles.contains(article.title)) {
						continue;
					}

					FieldOfInterest existingByUuid = Tema1.uuidArticlesSelection.get(article.uuid);
					FieldOfInterest existingByTitle = Tema1.titleArticlesSelection.get(article.title);

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

		} catch (IOException e) {
			System.err.println("Eroare citire JSON (" + fileName + "): " + e.getMessage());
		}
	}

	private void processUniqueArticle(FieldOfInterest article) {
		if (article.categories != null) {
			Set<String> distinctCategories = new HashSet<>(article.categories);

			for (String category : distinctCategories) {
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

	private void processKeywords(String text) {
		String content = text.toLowerCase();
		String[] words = content.split("[\\s]+");

		Set<String> uniqueWordsInArticle = new HashSet<>();

		for (String rawWord : words) {
			String cleanWord = rawWord.replaceAll("[^a-z]", "");

			if (!cleanWord.isEmpty() && !Tema1.excludedWords.contains(cleanWord)) {
				uniqueWordsInArticle.add(cleanWord);
			}
		}

		for (String word : uniqueWordsInArticle) {
			Tema1.keywordCounts
					.computeIfAbsent(word, k -> new AtomicInteger(0))
					.incrementAndGet();
		}
	}
}