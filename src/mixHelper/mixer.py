import json
import shutil
import os

# Function to copy and rename files
def copy_and_rename_files(folder, file_extension, total_copies):
    for original_num in range(1, 19):
        for copy_index in range(total_copies):
            new_num = original_num + copy_index * 18
            if new_num == original_num:  # Skip copying onto itself
                continue
            original_file = os.path.join(folder, f"{original_num}.{file_extension}")
            new_file = os.path.join(folder, f"{new_num}.{file_extension}")
            shutil.copyfile(original_file, new_file)

            # If JSON files, update the edition number
            if file_extension == "json":
                with open(new_file, "r+") as file:
                    data = json.load(file)
                    data["edition"] = new_num
                    file.seek(0)
                    json.dump(data, file, indent=2)
                    file.truncate()

# Paths to the image and json folders
image_folder = 'images'
json_folder = 'json'

# Copy and rename image files
copy_and_rename_files(image_folder, "png", 12)  # Total of 12 copies including original

# Copy and rename json files and update metadata
copy_and_rename_files(json_folder, "json", 12)  # Total of 12 copies including original
