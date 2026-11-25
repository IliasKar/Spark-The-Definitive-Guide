import boto3

def update_crawler_with_delta_targets(crawler_name, new_delta_targets):
    glue_client = boto3.client('glue', region_name='eu-central-1')

    # Get the existing crawler configuration
    response = glue_client.get_crawler(Name=crawler_name)
    existing_crawler_config = response['Crawler']

    # Get existing Delta targets
    existing_delta_targets = existing_crawler_config['Targets'].get('DeltaTargets', [])
    print(existing_delta_targets)
    
    # Extract existing paths from existing delta targets
    existing_paths = [path for target in existing_delta_targets for path in target['DeltaTables']]


    path_existed = False
    for new_path in new_delta_targets:
        if new_path in existing_paths:
            path_existed = True
            print(f"Path '{new_path}' already exists in the crawler's delta targets.")

    # Check if new paths already exist in Delta targets
    paths_to_add = []
    if not path_existed:
        # Iterate over existing delta targets and count the number of paths in each
        for target in existing_delta_targets:
            delta_tables = target.get('DeltaTables', [])
            if len(delta_tables) < 10:
                paths_to_add.extend(new_delta_targets)
                target['DeltaTables'].extend(new_delta_targets)
                break  # Exit loop if a target with less than 10 paths is found

        if not paths_to_add:
            # If no target with less than 10 paths was found, create a new delta target
            new_target = {"DeltaTables": new_delta_targets,"CreateNativeDeltaTable": True }
            existing_delta_targets.append(new_target)
            paths_to_add.extend(new_delta_targets)

        # Convert the list of strings to the format expected by DeltaTargets
        formatted_delta_targets = [{"DeltaTables": [path]} for path in paths_to_add]

        # Update the crawler with the modified configuration
        response = glue_client.update_crawler(
            Name=crawler_name,
            Role=existing_crawler_config['Role'],
            DatabaseName=existing_crawler_config['DatabaseName'],
            Targets={'DeltaTargets': existing_delta_targets},
        )
        print(f"Crawler '{crawler_name}' updated successfully with DeltaTargets: {formatted_delta_targets}")
    
    return response