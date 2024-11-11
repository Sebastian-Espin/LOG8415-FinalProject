# Reads dataset.txt with the form <User><TAB><Friends> and returns {userId, list<friends> (integer)}
def process_dataset():
    user_friends_dict = {}
    with open('dataset.txt') as f:
        for line in f:
            line_string = line.strip().split("\t")
            user = int(line_string[0])
            if len(line_string) == 1:
                user_friends_dict[user] = []
            else:
                user_friends_dict[user] = list(map(int, line_string[1].split(",")))
    return user_friends_dict

# Mapper function: emit pairs (user, potential_friend) for second-degree connections.
def map_dataset(user_friends_dict: dict, partial_users: dict):
    mapped = []
    for user in partial_users:
        friends = user_friends_dict[user]
        for friend in friends:
            for mutual_friend in user_friends_dict.get(friend, []):
                if mutual_friend != user and mutual_friend not in friends:
                    mapped.append(((user, mutual_friend), 1))
    return mapped

# Reducer function: aggregate pairs by summing mutual connections.
def reduce_dataset(mapped_data):
    from collections import defaultdict

    # Aggregate counts of mutual friends
    mutual_counts = defaultdict(int)
    for (user, mutual_friend), count in mapped_data:
        mutual_counts[(user, mutual_friend)] += count

    # Create recommendations for each user based on the mutual counts
    recommandations_dict = defaultdict(list)
    for (user, mutual_friend), count in mutual_counts.items():
        recommandations_dict[user].append((mutual_friend, count))

    # Sort and retain top 10 recommendations per user
    for user in recommandations_dict:
        recommandations_dict[user].sort(key=lambda x: (-x[1], x[0]))  # Sort by count (desc) and userId (asc)
        recommandations_dict[user] = [friend for friend, _ in recommandations_dict[user][:10]]

    return recommandations_dict

# Function to split the dataset into three parts for parallel mapping simulation.
def split_dataset(user_friends_dict):
    items = list(user_friends_dict.items())
    split_size = len(items) // 3
    part1 = dict(items[:split_size])
    part2 = dict(items[split_size:2 * split_size])
    part3 = dict(items[2 * split_size:])
    return part1, part2, part3

# Outputs to the terminal and creates a file recommandations.txt that 
# contains the recommendations for each user in the dataset.
def output():
    user_friends_dict = process_dataset()
    
    # Split the dataset into three parts for the mapping stage.
    part1, part2, part3 = split_dataset(user_friends_dict)

    # Apply the map function on each part of the dataset, using the entire dataset for context.
    mapped_part1 = map_dataset(user_friends_dict, part1)
    mapped_part2 = map_dataset(user_friends_dict, part2)
    mapped_part3 = map_dataset(user_friends_dict, part3)

    # Combine all mapped results (simulating data from different instances)
    combined_mapped = mapped_part1 + mapped_part2 + mapped_part3

    # Apply the reduce function on the combined mapped data
    recommandations_dict = reduce_dataset(combined_mapped)

    # Display the results for specific users
    users_to_present = [924, 8941, 8942, 9019, 9020, 9021, 9022, 9990, 9992, 9993]
    for user in users_to_present:
        print(f"User {user}: {recommandations_dict[user]}")
    # with open('recommandations.txt', 'w') as f:
    #     for user in recommandations_dict:
    #         f.write(f"{user}\t{','.join(map(str, recommandations_dict[user]))}\n")

if __name__ == "__main__":
    output()
