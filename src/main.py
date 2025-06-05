from extract.extract_eo import ExtractEO
from extract.extract_ciro import ExtractCIRO
def main():
    extractor = ExtractEO()
    extractor.run()
    
    #extractor = ExtractCIRO()
    #extractor.run()

if __name__ == "__main__":
    main()