IMAGE=eneco/kafka-connect-tennet
TAG=$(git describe --dirty)

if [ "$TAG" ]; then
  docker login -u="$DOCKER_USER" -p="$DOCKER_PASS" && \
  docker build -t $IMAGE . && \
  docker tag $IMAGE $IMAGE:latest && \
  docker tag $IMAGE $IMAGE:$TAG && \
  docker push $IMAGE:latest && \
  docker push $IMAGE:$TAG && \
  echo published tagged commit $IMAGE:$TAG
else
  echo not publishing untagged commit.
fi
