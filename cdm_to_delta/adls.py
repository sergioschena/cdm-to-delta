from typing import List

from azure.storage.blob import BlobClient, BlobBlock, BlockState


def copy_range_from_url(
    source_url: str,
    target_blob_client: BlobClient,
    source_authorization: str,
    offset: int,
    length: int,
    append: bool = False,
    max_block_size: int = 100 * 1024 * 1024,
    max_uncommited_blocks: int = 50000,
) -> None:
    """
    Copies a range of bytes from the source blob to a destination one.

    :param source_url: str
      The URL of the source file.
    :param target_blob_client: :class:`BlobClient`
      The BlobClient representing the target blob.
    :param source_authorization: str
      The authorization token for the source URL.
    :param offset: int
      The starting byte position of the data to be copied.
    :param length: int
      The length of the data to be copied, in bytes.
    :param max_block_size: int (optional)
      The maximum size of each block during the copying process. The default value is 100MB (100 * 1024 * 1024 bytes).
    :param max_uncommited_blocks: int (optional)
      The maximum number of uncommitted blocks before committing them. The default value is 50000.
    """

    if offset < 0:
        raise ValueError("Offset cannot be negative")
    if length <= 0:
        raise ValueError("Length must be positive")

    MIN_MAXBLOCKSIZE = 1
    MAX_MAXBLOCKSIZE = 4000 * 1024 * 1024

    if max_block_size < MIN_MAXBLOCKSIZE or max_block_size > MAX_MAXBLOCKSIZE:
        raise ValueError(f"Max block size must be between {MIN_MAXBLOCKSIZE} and {MAX_MAXBLOCKSIZE}")

    MIN_MAXUNCOMMITEDBLOCKS = 1
    MAX_MAXUNCOMMITEDBLOCKS = 50000

    if max_uncommited_blocks < MIN_MAXUNCOMMITEDBLOCKS or max_uncommited_blocks > MAX_MAXBLOCKSIZE:
        raise ValueError(
            f"Max block size must be between {MIN_MAXUNCOMMITEDBLOCKS} and {MAX_MAXUNCOMMITEDBLOCKS}"
        )

    block_list: List[BlobBlock] = list()

    # If we are appending, let's obtain the list of existing blocks
    if append and target_blob_client.exists():
        blocks: List[BlobBlock] = target_blob_client.get_block_list()[0]
        block_list.extend(blocks)

    target = offset + length
    uncommitted_blocks = 0

    # We copy block-by-block until we reach the target.
    # Blocks are first staged and then committed to actually update the file.
    while offset < target:
        block_lenght = target - offset
        block_lenght = max_block_size if block_lenght > max_block_size else block_lenght
        block_id = f"{len(block_list):010}"
        block = BlobBlock(block_id, BlockState.LATEST)
        target_blob_client.stage_block_from_url(
            block_id,
            source_url,
            offset,
            block_lenght,
            source_authorization=source_authorization,
        )
        block_list.append(block)
        uncommitted_blocks += 1

        if uncommitted_blocks == max_uncommited_blocks:
            target_blob_client.commit_block_list(block_list)
            uncommitted_blocks = 0
        offset += block_lenght

    if uncommitted_blocks > 0:
        target_blob_client.commit_block_list(block_list)
