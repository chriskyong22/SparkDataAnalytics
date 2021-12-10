netflixPath = /common/users/cy287/NetflixData
redditPath = /common/users/cy287/RedditData

all: hour photo movie graph
	echo "done";
	
directories:
	mkdir -p target/output;
    
movie: directories
	spark-submit target/NetflixMovieAverage.jar ${netflixPath}/NetflixData-ExtraSmall.csv > target/output/NetflixData-ExtraSmallAveragesOutput; 
	spark-submit target/NetflixMovieAverage.jar ${netflixPath}/NetflixData-Small.csv > target/output/NetflixData-SmallAveragesOutput; 
	spark-submit target/NetflixMovieAverage.jar ${netflixPath}/NetflixData-Medium.csv > target/output/NetflixData-MediumAveragesOutput; 
	spark-submit target/NetflixMovieAverage.jar ${netflixPath}/NetflixData-Large.csv > target/output/NetflixData-LargeAveragesOutput; 

graph: directories
	cd target;
	spark-submit target/NetflixGraphGenerate.jar ${netflixPath}/NetflixData-ExtraSmall.csv > target/output/NetflixData-ExtraSmallGraphOutput;
	# spark-submit target/NetflixGraphGenerate.jar ${netflixPath}/NetflixData-Small.csv > target/output/NetflixData-SmallGraphOutput; 
	
hour: directories
	cd target;
	spark-submit target/RedditHourImpact.jar ${redditPath}/RedditData-Large.csv > target/output/RedditData-LargeHourOutput;
	spark-submit target/RedditHourImpact.jar ${redditPath}/RedditData-Medium.csv > target/output/RedditData-MediumHourOutput;
	spark-submit target/RedditHourImpact.jar ${redditPath}/RedditData-Small.csv > target/output/RedditData-SmallHourOutput;

photo: directories
	cd target;
	spark-submit target/RedditPhotoImpact.jar ${redditPath}/RedditData-Large.csv > target/output/RedditData-LargePhotoOutput;
	spark-submit target/RedditPhotoImpact.jar ${redditPath}/RedditData-Medium.csv > target/output/RedditData-MediumPhotoOutput;
	spark-submit target/RedditPhotoImpact.jar ${redditPath}/RedditData-Small.csv > target/output/RedditData-SmallPhotoOutput;
    
clean:
	rm -rf target/output
