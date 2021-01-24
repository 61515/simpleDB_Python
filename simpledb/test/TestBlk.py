from simpledb.file.BlockId import BlockId


if __name__ == '__main__':
    blk = BlockId("file", 1)
    buffers = {blk: 1}
    pins = [blk]
    # pins.remove(blk)
    if blk in pins:
        print(1)
    print(pins.count(blk))
    # print(pins)
    # print(buffers.get(blk))
