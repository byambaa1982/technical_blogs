
# Clustering Financial Advice Texts Using TF-IDF and K-Means

When analyzing user-generated recommendations or advice, clustering similar entries is a time-tested way to distill key themes. In this post, we walk through a simple but powerful approach: using **TF-IDF** vectorization and **K-Means** clustering to group financial advice snippets based on their textual similarity.

## Dataset Overview

Our dataset comprises user-provided financial advice. Each entry includes a `user_id` and their corresponding `user_advice`. For this demonstration, we use five examples:

| user_id | user_advice                 |
|---------|-----------------------------|
| 1       | Save money regularly        |
| 2       | Invest in stocks            |
| 3       | Put money aside each month  |
| 4       | Buy shares for long term    |
| 5       | Save cash consistently      |

## Clustering Process

### Step 1: Text Vectorization with TF-IDF

To quantitatively compare textual data, we convert each `user_advice` entry into a TF-IDF vector. This method emphasizes important words while minimizing the impact of commonly used (stop) words.

```python
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import normalize

vectorizer = TfidfVectorizer(stop_words='english', max_features=1000)
X = vectorizer.fit_transform(df['user_advice'])
X = normalize(X)
```

### Step 2: K-Means Clustering

We apply K-Means clustering to the normalized TF-IDF vectors. Here, we set `num_clusters=3` to capture three primary themes.

```python
from sklearn.cluster import KMeans

num_clusters = 3
kmeans = KMeans(n_clusters=num_clusters, n_init=10, random_state=42)
df['cluster'] = kmeans.fit_predict(X)
```

### Step 3: Results Aggregation

After assigning each advice snippet to a cluster, we aggregate to see how many entries belong to each group.

**Cluster Counts Table:**

| cluster | count |
|---------|-------|
|   0     | 3     |
|   1     | 1     |
|   2     | 1     |

## Cluster Details

Let's examine the actual contents of each cluster:

#### Cluster 0 (Count: 3)
- Save money regularly
- Put money aside each month
- Save cash consistently

#### Cluster 1 (Count: 1)
- Invest in stocks

#### Cluster 2 (Count: 1)
- Buy shares for long term

## Insights

- **Cluster 0:** Advice focusing on consistent saving habits.
- **Cluster 1:** Centers on investing in stocks.
- **Cluster 2:** Emphasizes long-term investing via buying shares.

This small example illustrates how unsupervised learning can uncover related themes in user text inputs with minimal manual effort.

## Output Result Tables

**1. Input Data**

| user_id | user_advice                 |
|---------|-----------------------------|
| 1       | Save money regularly        |
| 2       | Invest in stocks            |
| 3       | Put money aside each month  |
| 4       | Buy shares for long term    |
| 5       | Save cash consistently      |

**2. Cluster Assignments**

| user_id | user_advice                 | cluster |
|---------|-----------------------------|---------|
| 1       | Save money regularly        |    0    |
| 2       | Invest in stocks            |    1    |
| 3       | Put money aside each month  |    0    |
| 4       | Buy shares for long term    |    2    |
| 5       | Save cash consistently      |    0    |

**3. Cluster Counts**

| cluster | count |
|---------|-------|
|   0     | 3     |
|   1     | 1     |
|   2     | 1     |

---

### Conclusion

K-Means clustering, combined with TF-IDF text vectorization, allows us to group conceptually similar financial advice. This methodology can be scaled to much larger datasets to extract practical themes, inform moderation, or guide content strategy.

