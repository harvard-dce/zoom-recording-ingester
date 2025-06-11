FROM public.ecr.aws/lambda/python:3.12

COPY requirements.txt ${LAMBDA_TASK_ROOT}
RUN pip install --no-cache-dir -r ./requirements.txt
RUN dnf install -y tar xz

RUN curl -O -L https://github.com/BtbN/FFmpeg-Builds/releases/download/latest/ffmpeg-master-latest-linux64-gpl.tar.xz && \
  tar -xvf ffmpeg-master-latest-linux64-gpl.tar.xz && \
  cd ffmpeg-master-*-linux64-gpl && \
  mv bin/ffprobe ${LAMBDA_TASK_ROOT} && \
  cd .. && \
  rm -rf ffmpeg-*

COPY . ${LAMBDA_TASK_ROOT}

# CMD will be set when CDK creates each function
