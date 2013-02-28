from pybloomfilter import BloomFilter

bf = BloomFilter(10000000, 0.999999999, "filter.bloom")
total = 0
count = 0
with open("whitelist.txt", "r") as f:
    for domain in f:
        total += 1
        if(bf.add(domain.rstrip())):
            count += 1
            print(domain.rstrip())

print(total)
print(count)