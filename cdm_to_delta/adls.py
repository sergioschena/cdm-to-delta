from typing import List

from azure.storage.blob import BlobClient, BlobBlock, BlockState


def copy_range_from_url(
    source_url: str,
    target_blob_client: BlobClient,
    source_authorization: str,
    offset: int,
    length: int,
    # append: bool = False,
    max_block_size: int = 100 * 1024 * 1024,
    max_uncommited_blocks: int = 50000,
):
    if offset < 0:
        raise ValueError("Offset cannot be negative")
    if length <= 0:
        raise ValueError("Length must be positive")

    MIN_MAXBLOCKSIZE = 1
    MAX_MAXBLOCKSIZE = 4000 * 1024 * 1024

    if max_block_size < MIN_MAXBLOCKSIZE or max_block_size > MAX_MAXBLOCKSIZE:
        raise ValueError(
            f"Max block size must be between {MIN_MAXBLOCKSIZE} and {MAX_MAXBLOCKSIZE}"
        )

    MIN_MAXUNCOMMITEDBLOCKS = 1
    MAX_MAXUNCOMMITEDBLOCKS = 50000

    if (
        max_uncommited_blocks < MIN_MAXUNCOMMITEDBLOCKS
        or max_uncommited_blocks > MAX_MAXBLOCKSIZE
    ):
        raise ValueError(
            f"Max block size must be between {MIN_MAXUNCOMMITEDBLOCKS} and {MAX_MAXUNCOMMITEDBLOCKS}"
        )

    block_list: List[BlobBlock] = list()

    # if append and blob_client.exists(target_uri):
    #     blocks: List[BlobBlock] = blob_client.get_block_list(target_uri)[0]
    #     block_list.append(blocks)

    target = offset + length
    uncommitted_blocks = 0

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
