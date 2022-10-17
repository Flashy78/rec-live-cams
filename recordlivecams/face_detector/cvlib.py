from pathlib import Path

import cv2

import cvlib as cv


def detect_faces(
    username: str, image_path: Path, thumb_folder: Path, new_folder: Path
) -> int:
    image = cv2.imread(str(image_path))
    faces, confidences = cv.detect_face(image)

    if len(faces) >= 2:
        move_to_folder = new_folder / image_path.parent.name
        move_to_folder.mkdir(parents=True, exist_ok=True)
        # Move all images from that streamer into a different folder
        for file in new_folder.rglob(f"**/{username}-*"):
            file.rename(move_to_folder / file.name)
        for file in thumb_folder.rglob(f"**/{username}-*"):
            file.rename(move_to_folder / file.name)

    return faces
