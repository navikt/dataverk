def as_markdown(blob):
    items = []

    items.append(f"### Dataset:\n\n")

    items.append(f"### Blob:\n\n")
    items.append(f"Blob: _{blob.name}_\n\n")
    items.append(f"Bucket: _{blob.bucket.name}_\n\n")
    items.append(f"Storage class: _{blob.storage_class}_\n\n")
    items.append(f"ID: _{blob.id}_\n\n")
    items.append(f"Size: _{blob.size}_\n\n")
    items.append(f"Updated: _{blob.updated}_\n\n")
    items.append(f"Generation: _{blob.generation}_\n\n")
    items.append(f"Metageneration: _{blob.metageneration}_\n\n")
    items.append(f"Etag: _{blob.etag}_\n\n")
    items.append(f"Owner: _{blob.owner}_\n\n")
    items.append(f"Component count: _{blob.component_count}_\n\n")
    items.append(f"Crc32c: _{blob.crc32c}_\n\n")
    items.append(f"md5_hash: _{blob.md5_hash}_\n\n")
    items.append(f"Cache-control: _{blob.cache_control}_\n\n")
    items.append(f"Content-type: _{blob.content_type}_\n\n")
    items.append(f"Content-disposition: _{blob.content_disposition}_\n\n")
    items.append(f"Content-encoding: _{blob.content_encoding}_\n\n")
    items.append(f"Content-language: _{blob.content_language}_\n\n")

    items.append(f"### Metadata:\n\n")

    if blob.metadata is not None:
        for key, value in blob.metadata.items():
            items.append(f"{key}: _{value}_")
            items.append(f"\n\n")

    return "".join(items)


def as_json(blob):
    return {
        "id": blob.id,
        "size": blob.size,
        "name": blob.name,
        "content_encoding": blob.content_encoding,
        "cache_control": blob.cache_control,
        "metadata": blob.metadata,
        "public_url": blob.public_url,
        "bucket": blob.bucket.name,
        "storage_class": blob.storage_class,
        "updated": blob.updated,
        "generation": blob.generation,
        "metageneration": blob.metageneration,
        "etag": blob.etag,
        "owner": blob.owner,
        "component_count": blob.component_count,
        "crc32c": blob.crc32c,
        "md5_hash": blob.md5_hash,
        "content_type": blob.content_type,
        "content_disposition": blob.content_disposition,
        "content_language": blob.content_language
    }
